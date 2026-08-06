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
        ):
            with self.subTest(argv=argv):
                decision = evaluate(argv)
                self.assertFalse(decision.allowed)
                self.assertEqual("identity.caller-supplied-impersonation", decision.rule_id)

    def test_git_and_gh_are_not_this_gates_business(self):
        # The artifact plane is where the agent is supposed to write, and the
        # git workspace lease already governs it.
        self.assertTrue(evaluate(["git", "push", "--force-with-lease"]).allowed)
        self.assertTrue(evaluate(["gh", "pr", "create", "--fill"]).allowed)


if __name__ == "__main__":
    unittest.main()
