import unittest

from command_policy import evaluate, _gcloud_words


class KubectlReadOnlyTest(unittest.TestCase):
    """The verbs an agent may run against a customer's cluster."""

    def test_a_plain_read_is_allowed(self):
        self.assertTrue(evaluate(["kubectl", "get", "pods"]).allowed)

    def test_a_mutating_verb_is_refused(self):
        decision = evaluate(["kubectl", "delete", "namespace", "prod"])
        self.assertFalse(decision.allowed)
        self.assertEqual("kubernetes.read-only", decision.rule_id)

    def test_a_global_flag_does_not_hide_the_verb(self):
        cases = (
            ["kubectl", "--namespace=kube-system", "get", "pods"],
            ["kubectl", "-n", "kube-system", "get", "pods"],
            ["kubectl", "--context", "gke_p_us-central1_c", "get", "nodes"],
        )
        for argv in cases:
            with self.subTest(argv=argv):
                self.assertTrue(evaluate(argv).allowed)

    def test_a_flag_value_is_not_mistaken_for_a_verb(self):
        # `--kubeconfig delete` must not read as the verb `delete`, and must
        # not read as an allowed verb either -- there is no verb here at all.
        decision = evaluate(["kubectl", "--kubeconfig", "delete"])
        self.assertFalse(decision.allowed)
        self.assertEqual("kubernetes.unreadable-command", decision.rule_id)

        # Even if the flag value is an allowed verb, it should not be accepted.
        decision = evaluate(["kubectl", "--kubeconfig", "get"])
        self.assertFalse(decision.allowed)
        self.assertEqual("kubernetes.unreadable-command", decision.rule_id)

    def test_a_subcommand_decides_where_the_verb_alone_cannot(self):
        self.assertTrue(evaluate(["kubectl", "rollout", "status", "deploy/api"]).allowed)
        self.assertFalse(evaluate(["kubectl", "rollout", "restart", "deploy/api"]).allowed)

    def test_an_argv_with_no_verb_is_refused_rather_than_shrugged_at(self):
        decision = evaluate(["kubectl"])
        self.assertFalse(decision.allowed)
        self.assertEqual("kubernetes.unreadable-command", decision.rule_id)

    def test_agent_supplied_impersonation_is_refused(self):
        # Impersonation is the broker's mechanism. A session that picks its own
        # principal is the model inverted.
        for argv in (
            ["kubectl", "--as", "admin@corp.com", "get", "secrets"],
            ["kubectl", "--as=admin@corp.com", "get", "secrets"],
            ["kubectl", "--as-group=system:masters", "get", "secrets"],
            ["kubectl", "--as-user-extra=scopes=admin", "get", "secrets"],
        ):
            with self.subTest(argv=argv):
                decision = evaluate(argv)
                self.assertFalse(decision.allowed)
                self.assertEqual("identity.caller-supplied-impersonation", decision.rule_id)

    def test_unknown_flags_are_refused_as_unreadable(self):
        # An unknown flag could be anything in a future kubectl release. We
        # refuse to guess whether it hides the verb, so treat it as unreadable.
        decision = evaluate(["kubectl", "--not-a-real-flag", "get", "pods"])
        self.assertFalse(decision.allowed)
        self.assertEqual("kubernetes.unreadable-command", decision.rule_id)

    def test_unlisted_value_taking_flags_do_not_hide_the_verb(self):
        # An unknown flag that takes a value (like a new flag in a future kubectl
        # release) should not allow a command like `kubectl --future-flag get delete pods`
        # to be interpreted as the mutating verb `delete`. Unknown flags are
        # unreadable; they're not treated as bare words.
        decision = evaluate(["kubectl", "--future-flag", "get", "delete", "pods"])
        self.assertFalse(decision.allowed)
        self.assertEqual("kubernetes.unreadable-command", decision.rule_id)

    def test_boolean_global_flags_do_not_hide_the_verb(self):
        # Boolean flags like --insecure-skip-tls-verify do not consume the next
        # token, so the verb appears immediately after. This test pins the
        # _KUBECTL_BOOLEAN_FLAGS constant.
        decision = evaluate(["kubectl", "--insecure-skip-tls-verify", "get", "pods"])
        self.assertTrue(decision.allowed)

    def test_command_specific_flags_do_not_hide_the_verb(self):
        # Flags after the verb cannot hide it, so we stop looking for a subcommand
        # when we encounter one. These are all legitimate read commands.
        cases = (
            ["kubectl", "logs", "-f", "mypod"],
            ["kubectl", "logs", "--tail=100", "mypod"],
            ["kubectl", "get", "-o", "wide", "pods"],
            ["kubectl", "get", "--all-namespaces", "pods"],
            ["kubectl", "describe", "-l", "app=x", "pods"],
            ["kubectl", "events", "--for", "pod/x"],
        )
        for argv in cases:
            with self.subTest(argv=argv):
                self.assertTrue(evaluate(argv).allowed)

    def test_false_refusal_on_unknown_command_specific_flag(self):
        # Once we have the verb, an unknown flag cannot hide it. But stopping at
        # the flag means we don't get a second word, so a two-word verb is refused.
        # This is intentional: the alternative (skipping unknown flags) reopens the
        # hole. `rollout --unknown status x` is false-refused, which is acceptable.
        decision = evaluate(["kubectl", "rollout", "--unknown", "status", "x"])
        self.assertFalse(decision.allowed)
        self.assertEqual("kubernetes.read-only", decision.rule_id)

    def test_global_flags_between_verb_and_subcommand_are_skipped(self):
        # Known global flags can appear between the verb and subcommand, and we
        # skip them to find the subcommand. This allows real kubectl commands like
        # `rollout -n prod status` to work correctly.
        cases = (
            (["kubectl", "rollout", "-n", "prod", "status", "deploy/x"], True, "rollout -n status"),
            (["kubectl", "auth", "-n", "prod", "can-i", "create", "pods"], True, "auth -n can-i"),
            (["kubectl", "config", "--kubeconfig", "f", "view"], True, "config --kubeconfig view"),
        )
        for argv, expected_allowed, desc in cases:
            with self.subTest(desc=desc):
                self.assertEqual(evaluate(argv).allowed, expected_allowed, desc)

    def test_unknown_flags_between_verb_and_subcommand_stop_parsing(self):
        # An unknown flag between the verb and subcommand stops us from finding
        # the subcommand, so a two-word verb is refused. This is safe because
        # the unknown flag could have arbitrary arity.
        decision = evaluate(["kubectl", "rollout", "-n", "status", "restart", "x"])
        self.assertFalse(decision.allowed)
        self.assertEqual("kubernetes.read-only", decision.rule_id)

    def test_adjacent_subcommands_still_work(self):
        # Subcommands that appear immediately after the verb are still found and
        # evaluated correctly.
        cases = (
            (["kubectl", "rollout", "status", "deploy/x"], True, "rollout status"),
            (["kubectl", "rollout", "restart", "deploy/x"], False, "rollout restart"),
            (["kubectl", "config", "current-context"], True, "config current-context"),
            (["kubectl", "get", "pods", "-o", "wide"], True, "get pods with flag after"),
        )
        for argv, expected_allowed, desc in cases:
            with self.subTest(desc=desc):
                self.assertEqual(evaluate(argv).allowed, expected_allowed, desc)

    def test_exec_is_read_only_refused(self):
        # exec is mutating (it runs arbitrary code in the container).
        decision = evaluate(["kubectl", "exec", "pod", "--", "rm", "-rf", "/"])
        self.assertFalse(decision.allowed)
        self.assertEqual("kubernetes.read-only", decision.rule_id)

    def test_git_and_gh_are_not_this_gates_business(self):
        # The artifact plane is where the agent is supposed to write, and the
        # git workspace lease already governs it.
        self.assertTrue(evaluate(["git", "push", "--force-with-lease"]).allowed)
        self.assertTrue(evaluate(["gh", "pr", "create", "--fill"]).allowed)

    def test_all_kubectl_read_verbs_are_reachable(self):
        # Literal list of all single and multi-word verbs allowed for kubectl.
        # This test is independent of KUBECTL_READ_VERBS, so deleting a verb
        # breaks this test.
        allowed_verbs = [
            (["kubectl", "api-resources"], "api-resources"),
            (["kubectl", "api-versions"], "api-versions"),
            (["kubectl", "cluster-info"], "cluster-info"),
            (["kubectl", "describe", "node", "mynode"], "describe"),
            (["kubectl", "events"], "events"),
            (["kubectl", "explain", "pods"], "explain"),
            (["kubectl", "get", "pods"], "get"),
            (["kubectl", "logs", "mypod"], "logs"),
            (["kubectl", "top", "nodes"], "top"),
            (["kubectl", "version"], "version"),
            (["kubectl", "wait", "--for=condition=Ready", "pod/mypod"], "wait"),
            (["kubectl", "auth", "can-i", "get", "pods"], "auth can-i"),
            (["kubectl", "auth", "whoami"], "auth whoami"),
            (["kubectl", "config", "current-context"], "config current-context"),
            (["kubectl", "config", "get-contexts"], "config get-contexts"),
            (["kubectl", "config", "view"], "config view"),
            (["kubectl", "rollout", "history", "deploy/api"], "rollout history"),
            (["kubectl", "rollout", "status", "deploy/api"], "rollout status"),
        ]
        for argv, desc in allowed_verbs:
            with self.subTest(verb=desc):
                self.assertTrue(evaluate(argv).allowed, f"{desc} should be allowed")

    def test_all_kubectl_impersonation_flags_are_refused(self):
        # Literal list of all kubectl impersonation flags. This test is
        # independent of _IMPERSONATION_FLAGS, so deleting any flag breaks this test.
        impersonation_flags = [
            (["kubectl", "--as", "admin@corp.com", "get", "secrets"], "--as"),
            (["kubectl", "--as=admin@corp.com", "get", "secrets"], "--as="),
            (["kubectl", "--as-group", "system:masters", "get", "secrets"], "--as-group"),
            (["kubectl", "--as-group=system:masters", "get", "secrets"], "--as-group="),
            (["kubectl", "--as-uid", "1234", "get", "secrets"], "--as-uid"),
            (["kubectl", "--as-uid=1234", "get", "secrets"], "--as-uid="),
            (["kubectl", "--as-user-extra=scopes=admin", "get", "secrets"], "--as-user-extra="),
            (["kubectl", "--impersonate-service-account", "sa@proj.iam.gserviceaccount.com", "get", "secrets"], "--impersonate-service-account"),
            (["kubectl", "--impersonate-service-account=sa@proj.iam.gserviceaccount.com", "get", "secrets"], "--impersonate-service-account="),
        ]
        for argv, flag_desc in impersonation_flags:
            with self.subTest(flag=flag_desc):
                decision = evaluate(argv)
                self.assertFalse(decision.allowed)
                self.assertEqual("identity.caller-supplied-impersonation", decision.rule_id)


class GcloudReadOnlyTest(unittest.TestCase):
    """gcloud has no fixed verb position, so allowed command paths are listed."""

    def test_a_listed_read_command_is_allowed(self):
        self.assertTrue(evaluate(["gcloud", "container", "clusters", "list"]).allowed)

    def test_a_positional_argument_does_not_hide_the_command(self):
        argv = ["gcloud", "container", "clusters", "get-credentials", "prod-usc1"]
        self.assertTrue(evaluate(argv).allowed)

    def test_an_unlisted_command_is_refused(self):
        decision = evaluate(["gcloud", "container", "clusters", "delete", "prod-usc1"])
        self.assertFalse(decision.allowed)
        self.assertEqual("gcp.read-only", decision.rule_id)

    def test_a_flag_value_is_not_mistaken_for_a_command_word(self):
        # `--project delete` must not contribute `delete` to the command path,
        # and `--project` must not swallow `container` either.
        argv = ["gcloud", "--project", "my-proj", "container", "clusters", "list"]
        self.assertTrue(evaluate(argv).allowed)

    def test_an_unlisted_group_alone_is_refused(self):
        decision = evaluate(["gcloud", "compute", "instances", "list"])
        self.assertFalse(decision.allowed)
        self.assertEqual("gcp.read-only", decision.rule_id)

    def test_bare_gcloud_is_refused(self):
        decision = evaluate(["gcloud"])
        self.assertFalse(decision.allowed)
        self.assertEqual("gcp.read-only", decision.rule_id)

    def test_service_account_impersonation_is_refused(self):
        argv = ["gcloud", "--impersonate-service-account", "x@y.iam.gserviceaccount.com",
                "container", "clusters", "list"]
        decision = evaluate(argv)
        self.assertFalse(decision.allowed)
        self.assertEqual("identity.caller-supplied-impersonation", decision.rule_id)

    def test_flag_with_equals_syntax_is_handled(self):
        # Flags using = syntax should not consume the next token.
        argv = ["gcloud", "--project=my-proj", "container", "clusters", "list"]
        self.assertTrue(evaluate(argv).allowed)

    def test_unknown_flag_is_refused_as_unreadable(self):
        # An unknown global flag could take a value and hide the command path.
        # Without knowing its arity, we cannot safely read the argv, so we refuse
        # it as unreadable. This is fail-closed, consistent with kubectl.
        decision = evaluate(["gcloud", "--unknown-flag", "container", "clusters", "list"])
        self.assertFalse(decision.allowed)
        self.assertEqual("gcp.unreadable-command", decision.rule_id)

    def test_multiple_flags_before_command(self):
        # Multiple flags should be correctly skipped.
        argv = ["gcloud", "--project", "proj1", "--region", "us-central1",
                "container", "clusters", "list"]
        self.assertTrue(evaluate(argv).allowed)

    def test_config_list_is_allowed(self):
        # Test individual listed commands from GCLOUD_READ_COMMANDS.
        self.assertTrue(evaluate(["gcloud", "config", "list"]).allowed)

    def test_config_set_is_refused(self):
        # `config set` is not in GCLOUD_READ_COMMANDS, so it should be refused.
        decision = evaluate(["gcloud", "config", "set", "core.project", "my-proj"])
        self.assertFalse(decision.allowed)
        self.assertEqual("gcp.read-only", decision.rule_id)

    def test_version_command_is_allowed(self):
        # `version` is a single-word command in GCLOUD_READ_COMMANDS.
        self.assertTrue(evaluate(["gcloud", "version"]).allowed)

    def test_unknown_flag_hides_command_path(self):
        # When --trace-token is recognized as taking a value, it consumes 'list',
        # leaving [container, clusters, delete, my-cluster] which doesn't match
        # any allowed prefix, so it's refused as gcp.read-only. The exploit is
        # still blocked; the rule_id is just more informative now.
        decision = evaluate(["gcloud", "container", "clusters", "--trace-token", "list", "delete", "my-cluster"])
        self.assertFalse(decision.allowed)
        # Could be gcp.read-only (flag is recognized) or gcp.unreadable-command
        # (flag is unknown). Either way, it's refused.
        self.assertIn(decision.rule_id, {"gcp.read-only", "gcp.unreadable-command"})

    def test_trace_token_and_delete_exploit(self):
        # Regression test for exploit with --trace-token and delete.
        # Once --trace-token is recognized as consuming a value, the words become
        # [projects, delete, my-project] which doesn't match (projects, list),
        # so the exploit is blocked.
        decision = evaluate(["gcloud", "projects", "--trace-token", "list", "delete", "my-project"])
        self.assertFalse(decision.allowed)
        self.assertIn(decision.rule_id, {"gcp.read-only", "gcp.unreadable-command"})

    def test_flags_file_is_refused_outright(self):
        # --flags-file reads from a file under the agent's control. We cannot
        # safely scan that file (race condition), and it could contain hidden
        # flags like --impersonate-service-account, so refuse it outright.
        decision = evaluate(["gcloud", "--flags-file", "/tmp/ff.yaml", "container", "clusters", "list"])
        self.assertFalse(decision.allowed)
        self.assertEqual("gcp.flags-file-forbidden", decision.rule_id)

    def test_flags_file_with_equals_syntax_is_refused(self):
        # --flags-file=/path/to/file should also be refused.
        decision = evaluate(["gcloud", "--flags-file=/tmp/ff.yaml", "container", "clusters", "list"])
        self.assertFalse(decision.allowed)
        self.assertEqual("gcp.flags-file-forbidden", decision.rule_id)

    def test_flags_file_between_command_words_is_refused(self):
        # Even if --flags-file appears deep in the argv, it must be refused.
        decision = evaluate(["gcloud", "container", "--flags-file", "/tmp/ff.yaml", "clusters", "list"])
        self.assertFalse(decision.allowed)
        self.assertEqual("gcp.flags-file-forbidden", decision.rule_id)

    def test_all_gcloud_commands_in_allowlist_are_reachable(self):
        # Literal list of all commands that should be allowed. This test is
        # independent of GCLOUD_READ_COMMANDS, so deleting a command breaks
        # this test. Each command is tested with a realistic positional argument.
        allowed_commands = [
            (["gcloud", "auth", "list"], "auth list"),
            (["gcloud", "config", "get"], "config get"),
            (["gcloud", "config", "list"], "config list"),
            (["gcloud", "container", "clusters", "describe", "mycluster"], "container clusters describe"),
            (["gcloud", "container", "clusters", "list"], "container clusters list"),
            (["gcloud", "container", "clusters", "get-credentials", "prod-usc1"], "container clusters get-credentials"),
            (["gcloud", "container", "get-server-config"], "container get-server-config"),
            (["gcloud", "container", "node-pools", "describe", "default"], "container node-pools describe"),
            (["gcloud", "container", "node-pools", "list"], "container node-pools list"),
            (["gcloud", "info"], "info"),
            (["gcloud", "logging", "read"], "logging read"),
            (["gcloud", "projects", "describe", "myproj"], "projects describe"),
            (["gcloud", "projects", "get-iam-policy", "myproj"], "projects get-iam-policy"),
            (["gcloud", "projects", "list"], "projects list"),
            (["gcloud", "version"], "version"),
        ]
        for argv, desc in allowed_commands:
            with self.subTest(cmd=desc):
                self.assertTrue(evaluate(argv).allowed, f"{desc} should be allowed")

    def test_all_gcloud_flags_with_value_consume_their_values(self):
        # Literal list of all value-taking flags. This test is independent of
        # _GCLOUD_FLAGS_WITH_VALUE, so deleting a flag breaks this test. Each
        # flag is tested to ensure it skips the next token correctly.
        flags_with_value = [
            ("--project", "proj1"),
            ("--format", "json"),
            ("--filter", "name:foo"),
            ("--region", "us-central1"),
            ("--zone", "us-central1-a"),
            ("-z", "us-central1-a"),
            ("--location", "us-central1"),
            ("--account", "user@domain.com"),
            ("--configuration", "myconfig"),
            ("--verbosity", "debug"),
            ("--billing-project", "billingproj"),
            ("--sort-by", "name"),
            ("--limit", "10"),
            ("--trace-token", "token123"),
            ("--flatten", "name[]"),
            ("--access-token-file", "/path/to/token"),
            ("--page-size", "50"),
        ]
        for flag, value in flags_with_value:
            argv = ["gcloud", flag, value, "container", "clusters", "list"]
            with self.subTest(flag=flag):
                words = _gcloud_words(argv)
                self.assertIsNotNone(words, f"Flag {flag} should be recognized")
                self.assertEqual(words, ["container", "clusters", "list"],
                                f"Flag {flag} should consume '{value}', got {words}")

    def test_new_flags_trace_token_zone_etc(self):
        # Regression test for the five newly added flags: --trace-token,
        # --flatten, --access-token-file, -z, --page-size. These are real
        # gcloud flags and should be recognized as consuming values.
        test_cases = [
            (["gcloud", "container", "clusters", "describe", "-z", "us-central1-a", "mycluster"], True),
            (["gcloud", "container", "clusters", "--trace-token", "tok", "list"], True),
            (["gcloud", "container", "clusters", "--flatten", "x", "list"], True),
            (["gcloud", "container", "clusters", "list", "--page-size", "100"], True),
        ]
        for argv, expected_allowed in test_cases:
            with self.subTest(argv=argv):
                self.assertEqual(evaluate(argv).allowed, expected_allowed)

    def test_exploit_still_blocked_with_new_flags(self):
        # Ensure the five new flags don't reopen the exploit holes.
        # If -z eats 'list', words become [container, clusters, delete, c]
        # which doesn't match any allowed prefix.
        test_cases = [
            (["gcloud", "container", "clusters", "--trace-token", "list", "delete", "my-cluster"], False),
            (["gcloud", "container", "clusters", "-z", "list", "delete", "c"], False),
            (["gcloud", "container", "clusters", "--flatten", "list", "delete", "x"], False),
            (["gcloud", "container", "clusters", "--access-token-file", "list", "delete", "x"], False),
            (["gcloud", "container", "clusters", "--page-size", "list", "delete", "x"], False),
        ]
        for argv, expected_allowed in test_cases:
            with self.subTest(argv=argv):
                result = evaluate(argv)
                self.assertEqual(result.allowed, expected_allowed,
                                f"Command {argv} should be refused")

    def test_boolean_flags_do_not_hide_command(self):
        # Boolean flags like -q, -v, -h should not consume the next token,
        # so they don't hide the command path.
        test_cases = [
            (["gcloud", "container", "clusters", "list", "-q"], True),
            (["gcloud", "container", "clusters", "list", "-v"], True),
            (["gcloud", "container", "clusters", "list", "-h"], True),
            (["gcloud", "container", "clusters", "list", "--quiet"], True),
            (["gcloud", "container", "clusters", "list", "--version"], True),
            (["gcloud", "container", "clusters", "list", "--help"], True),
        ]
        for argv, expected_allowed in test_cases:
            with self.subTest(argv=argv):
                self.assertEqual(evaluate(argv).allowed, expected_allowed)

    def test_gcloud_identity_flags_are_refused(self):
        # All identity-changing flags should be refused outright:
        # - --access-token-file, --configuration, --account (documented)
        # - --credential-file-override, --authorization-token-file, --authority-selector
        #   (undocumented but accepted by gcloud, carry refreshable credentials)
        test_cases = [
            (["gcloud", "--access-token-file", "/tmp/tok.txt", "container", "clusters", "list"], "gcp.identity-change-forbidden"),
            (["gcloud", "--access-token-file=/tmp/tok.txt", "container", "clusters", "list"], "gcp.identity-change-forbidden"),
            (["gcloud", "--configuration", "evil", "container", "clusters", "list"], "gcp.identity-change-forbidden"),
            (["gcloud", "--configuration=evil", "container", "clusters", "list"], "gcp.identity-change-forbidden"),
            (["gcloud", "--account", "evil@corp.com", "container", "clusters", "list"], "gcp.identity-change-forbidden"),
            (["gcloud", "--account=evil@corp.com", "container", "clusters", "list"], "gcp.identity-change-forbidden"),
            # Hidden flags from calliope/cli.py (undocumented but real)
            (["gcloud", "--credential-file-override", "/tmp/key.json", "container", "clusters", "list"], "gcp.identity-change-forbidden"),
            (["gcloud", "--credential-file-override=/tmp/key.json", "container", "clusters", "list"], "gcp.identity-change-forbidden"),
            (["gcloud", "--authorization-token-file", "/tmp/tok", "container", "clusters", "list"], "gcp.identity-change-forbidden"),
            (["gcloud", "--authorization-token-file=/tmp/tok", "container", "clusters", "list"], "gcp.identity-change-forbidden"),
            (["gcloud", "--authority-selector", "x", "container", "clusters", "list"], "gcp.identity-change-forbidden"),
            (["gcloud", "--authority-selector=x", "container", "clusters", "list"], "gcp.identity-change-forbidden"),
        ]
        for argv, expected_rule_id in test_cases:
            with self.subTest(argv=argv):
                decision = evaluate(argv)
                self.assertFalse(decision.allowed)
                self.assertEqual(expected_rule_id, decision.rule_id)


if __name__ == "__main__":
    unittest.main()
