import unittest

from command_policy import evaluate


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
        # A real gcloud flag that's not in our enumeration should cause an
        # unreadable refusal. This is the exploit: --trace-token list delete
        # would otherwise read as container.clusters.list but the unknown flag
        # could hide other words.
        decision = evaluate(["gcloud", "container", "clusters", "--trace-token", "list", "delete", "my-cluster"])
        self.assertFalse(decision.allowed)
        self.assertEqual("gcp.unreadable-command", decision.rule_id)

    def test_trace_token_and_delete_exploit(self):
        # Regression test for exploit with --trace-token and delete.
        decision = evaluate(["gcloud", "projects", "--trace-token", "list", "delete", "my-project"])
        self.assertFalse(decision.allowed)
        self.assertEqual("gcp.unreadable-command", decision.rule_id)

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

    def test_each_gcloud_command_in_allowlist_is_reachable(self):
        # Regression test: each tuple in GCLOUD_READ_COMMANDS should be
        # reachable with a realistic argv. This catches mutations that empty
        # the allowlist or break the matching logic.
        from command_policy import GCLOUD_READ_COMMANDS
        for cmd_tuple in sorted(GCLOUD_READ_COMMANDS):
            argv = ["gcloud"] + list(cmd_tuple) + ["extra-arg"]
            with self.subTest(cmd=cmd_tuple):
                self.assertTrue(evaluate(argv).allowed,
                                f"Command {cmd_tuple} should be allowed")

    def test_each_gcloud_flag_with_value_consumes_its_value(self):
        # Regression test: each flag in _GCLOUD_FLAGS_WITH_VALUE should
        # consume the next token. If a flag is removed or its entry is broken,
        # the flag value gets treated as a command word and breaks the parsing.
        from command_policy import _GCLOUD_FLAGS_WITH_VALUE, _gcloud_words
        for flag in sorted(_GCLOUD_FLAGS_WITH_VALUE):
            if flag == "--impersonate-service-account":
                # Skip this one - it's also in _IMPERSONATION_FLAGS and gets
                # rejected early. We just need to check that it's in the flag
                # set for the sake of the enumeration.
                continue
            argv = ["gcloud", flag, "flagvalue", "container", "clusters", "list"]
            with self.subTest(flag=flag):
                words = _gcloud_words(argv)
                self.assertIsNotNone(words, f"Flag {flag} should be recognized")
                # The flag and its value should be skipped, leaving container onwards
                self.assertEqual(words, ["container", "clusters", "list"],
                                f"Flag {flag} should consume its value, got {words}")


if __name__ == "__main__":
    unittest.main()
