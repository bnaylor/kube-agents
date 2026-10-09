package main

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

// The real sources, relative to this package: the tests build the shipped
// tree from them rather than from fixtures, so a change to a cluster skill
// that breaks the session's copy fails here, not in an image build.
const (
	repoSkillsDir   = "../../../agents/cluster/skills"
	repoPersona     = "../../persona/session/persona.md"
	repoDockerfile  = "../../Dockerfile.worker"
	harnessHomeEnv  = "CLAUDE_CONFIG_DIR"
	buildOutFlag    = "-out"
	sessionToolPath = "./cmd/session-persona"
)

// writeCommandRE finds a command that changes a cluster or a project. It is a
// denylist, written independently of the transform's allowlists, so the two
// have to agree for the read-only test to pass.
var writeCommandRE = regexp.MustCompile(
	`\bkubectl\s+(apply|create|delete|patch|edit|replace|scale|autoscale|annotate|label|set|rollout|cordon|uncordon|drain|taint|expose|run|cp|exec)\b` +
		`|\bgcloud\b[^\n` + "`" + `]*\s(create|delete|update|upgrade|resize|add-iam-policy-binding|remove-iam-policy-binding|set-iam-policy|enable|disable|deploy)\b`)

// otherProgramRE finds a command line that runs a program the worker image
// does not ship.
var otherProgramRE = regexp.MustCompile(`^\s*(\$\s+)?(\./|python3?\s|git\s|jq\s|bash\s|sh\s)`)

func buildShipped(t *testing.T) string {
	t.Helper()
	out := t.TempDir()
	if err := build(repoSkillsDir, repoPersona, out); err != nil {
		t.Fatal(err)
	}
	return out
}

// The shipped tree is what Claude Code loads: CLAUDE.md at the root and one
// SKILL.md per skill whose frontmatter carries exactly the name, matching its
// directory, and the source's description. A field beyond those two would
// either be ignored or, like allowed-tools, widen what the skill may do.
func TestShippedTreeIsLoadableByClaudeCode(t *testing.T) {
	out := buildShipped(t)

	persona, err := os.ReadFile(filepath.Join(out, personaFile))
	if err != nil {
		t.Fatal(err)
	}
	src, _ := os.ReadFile(repoPersona)
	if string(persona) != string(src) {
		t.Error("CLAUDE.md is not the persona source byte for byte")
	}

	entries, err := os.ReadDir(filepath.Join(out, skillsDir))
	if err != nil {
		t.Fatal(err)
	}
	var shipped []string
	for _, e := range entries {
		shipped = append(shipped, e.Name())
	}
	if !slices.Equal(shipped, sessionSkills) {
		t.Fatalf("shipped skills = %v, want %v", shipped, sessionSkills)
	}

	for _, name := range sessionSkills {
		body, err := os.ReadFile(filepath.Join(out, skillsDir, name, skillFile))
		if err != nil {
			t.Fatal(err)
		}
		fm, rest, err := splitFrontmatter(body)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		var meta map[string]any
		if err := yaml.Unmarshal(fm, &meta); err != nil {
			t.Fatalf("%s: frontmatter: %v", name, err)
		}
		keys := make([]string, 0, len(meta))
		for k := range meta {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		if !slices.Equal(keys, []string{"description", "name"}) {
			t.Errorf("%s: frontmatter keys = %v, want exactly name and description", name, keys)
		}
		if meta["name"] != name {
			t.Errorf("%s: name = %v", name, meta["name"])
		}
		srcBody, _ := os.ReadFile(filepath.Join(repoSkillsDir, name, skillFile))
		srcFM, _, _ := splitFrontmatter(srcBody)
		var srcMeta skillFrontmatter
		_ = yaml.Unmarshal(srcFM, &srcMeta)
		if meta["description"] != srcMeta.Description || srcMeta.Description == "" {
			t.Errorf("%s: description changed in the build: %q", name, meta["description"])
		}
		if !strings.Contains(string(rest), sessionPreamble) {
			t.Errorf("%s: no session preamble", name)
		}
		// A literal, not clusterAgentOnlyMarker: the check must not move
		// with the constant it checks.
		for _, tool := range []string{"kanban_complete", "kanban_block"} {
			if strings.Contains(string(rest), tool) {
				t.Errorf("%s: still calls the cluster agent's %s", name, tool)
			}
		}
	}
}

// Every command in a shipped skill that changes a cluster or a project sits
// in a code block the propose-only note precedes, and none appears in prose
// or inline code where no note can reach it. A program the image does not
// ship is marked too. Checked against the real skills, so a cluster-skill
// edit that adds an unmarked write fails here.
func TestShippedSkillsMarkEveryWrite(t *testing.T) {
	out := buildShipped(t)
	writes := 0
	for _, name := range sessionSkills {
		body, err := os.ReadFile(filepath.Join(out, skillsDir, name, skillFile))
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(string(body), "\n")
		lastNote := ""
		inFence := false
		for i, l := range lines {
			trimmed := strings.TrimSpace(l)
			if strings.HasPrefix(trimmed, codeFence) {
				if !inFence {
					lastNote = precedingNote(lines, i)
				}
				inFence = !inFence
				continue
			}
			if writeCommandRE.MatchString(l) {
				writes++
				if !inFence {
					t.Errorf("%s:%d: a write outside a code block, where no note marks it: %s", name, i+1, trimmed)
				} else if lastNote != proposeOnlyNote {
					t.Errorf("%s:%d: a write in a block not marked propose-only: %s", name, i+1, trimmed)
				}
			}
			if inFence && otherProgramRE.MatchString(l) && lastNote != unavailableNote && lastNote != proposeOnlyNote {
				t.Errorf("%s:%d: a program the image does not ship, unmarked: %s", name, i+1, trimmed)
			}
		}
	}
	// The source skills do contain writes; zero would mean the scan matched
	// nothing, not that the skills are clean.
	if writes == 0 {
		t.Fatal("found no write commands at all; the scan is not reading the skills")
	}
}

// precedingNote returns the note line just above a code fence, skipping one
// blank line, or "".
func precedingNote(lines []string, fence int) string {
	for j := fence - 1; j >= 0 && j >= fence-2; j-- {
		if s := strings.TrimSpace(lines[j]); s != "" {
			return s
		}
	}
	return ""
}

// The persona names the skills the image ships, no more and no fewer, and
// says nothing that points the model at files: vamp-49's no-view check
// (gke-labs#2831) expects no Read, Glob or Grep on a cluster question, and
// skills load through the Skill tool.
func TestPersonaNamesTheShippedSkillsAndNoFiles(t *testing.T) {
	persona, err := os.ReadFile(repoPersona)
	if err != nil {
		t.Fatal(err)
	}
	p := string(persona)
	for _, name := range sessionSkills {
		if !strings.Contains(p, "`"+name+"`") {
			t.Errorf("persona does not name %s", name)
		}
	}
	for _, m := range regexp.MustCompile("`(gke-[a-z-]+)`").FindAllStringSubmatch(p, -1) {
		if !slices.Contains(sessionSkills, m[1]) {
			t.Errorf("persona names %s, which the image does not ship", m[1])
		}
	}
	for _, path := range []string{"SKILL.md", ".claude", "/home/node", "skills/"} {
		if strings.Contains(p, path) {
			t.Errorf("persona names a file path (%q); skills load through the Skill tool", path)
		}
	}
}

// The image builds the tree with this tool and copies it to the directory
// Claude Code reads, which the Dockerfile names as CLAUDE_CONFIG_DIR. Read
// from the Dockerfile itself; every extraction must find something.
func TestWorkerImageShipsTheTreeWhereTheHarnessReadsIt(t *testing.T) {
	raw, err := os.ReadFile(repoDockerfile)
	if err != nil {
		t.Fatal(err)
	}
	df := string(raw)

	home := regexp.MustCompile(harnessHomeEnv + `=(\S+)`).FindStringSubmatch(df)
	if home == nil {
		t.Fatalf("Dockerfile sets no %s", harnessHomeEnv)
	}
	run := regexp.MustCompile(`RUN go run ` + regexp.QuoteMeta(sessionToolPath) + `\s.*` + buildOutFlag + `\s+(\S+)`).FindStringSubmatch(df)
	if run == nil {
		t.Fatalf("Dockerfile does not run %s with %s", sessionToolPath, buildOutFlag)
	}
	if !strings.Contains(df, "COPY agents/cluster/skills/ ") {
		t.Error("Dockerfile does not copy agents/cluster/skills into the build stage")
	}
	for _, want := range []string{
		"COPY --from=build " + run[1] + "/" + personaFile + " " + home[1] + "/" + personaFile,
		"COPY --from=build " + run[1] + "/" + skillsDir + "/ " + home[1] + "/" + skillsDir + "/",
	} {
		if !strings.Contains(df, want) {
			t.Errorf("Dockerfile lacks %q", want)
		}
	}
	// After the chown, so the copies stay root-owned.
	if strings.Index(df, "chown -R node:node /home/node") > strings.Index(df, "COPY --from=build "+run[1]) {
		t.Error("the persona is copied before the chown, so the harness would own it")
	}
}

func TestTransformDropsFrontmatterBeyondNameAndDescription(t *testing.T) {
	src := "---\nname: demo\ndescription: A demo.\nallowed-tools: Bash\nhooks:\n  PreToolUse: []\n---\n\n# Demo\n\nBody.\n"
	out, err := transformSkill("demo", []byte(src))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(out), "allowed-tools") || strings.Contains(string(out), "hooks") {
		t.Errorf("a widening field survived:\n%s", out)
	}
}

func TestTransformRefusesABrokenSkill(t *testing.T) {
	for name, src := range map[string]string{
		"name mismatch":     "---\nname: other\ndescription: d\n---\n# T\n",
		"no description":    "---\nname: demo\n---\n# T\n",
		"no frontmatter":    "# T\n",
		"unclosed":          "---\nname: demo\ndescription: d\n# T\n",
		"bad yaml":          "---\nname: demo\ndescription: a: b: c\n---\n# T\n",
		"description > max": "---\nname: demo\ndescription: " + strings.Repeat("x", maxSkillDescriptionLen+1) + "\n---\n# T\n",
	} {
		if _, err := transformSkill("demo", []byte(src)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// The classifier is an allowlist: a kubectl or gcloud verb it does not know
// is a write, and a filter after a read is a pipe, not a missing program.
func TestClassifyBlock(t *testing.T) {
	for _, tc := range []struct {
		lang string
		body string
		want blockClass
	}{
		{"bash", "kubectl get pods -n x", blockRead},
		{"bash", "kubectl -n x describe pod p", blockRead},
		{"bash", "kubectl frobnicate pods", blockWrite},
		{"bash", "kubectl rollout restart deploy/x", blockWrite},
		{"bash", "gcloud container clusters describe c --region r", blockRead},
		{"bash", "gcloud logging read \"a | b\" --project p", blockRead},
		{"bash", "gcloud container clusters update c \\\n  --enable-x", blockWrite},
		{"bash", "gcloud services enable x", blockWrite},
		{"bash", "kubectl get deploy x -o yaml | grep probe", blockPipe},
		{"bash", "kubectl get cm x -o yaml | kubectl apply -f -", blockWrite},
		{"bash", "./scripts/audit.sh a b", blockUnavailable},
		{"bash", "# a comment\n\nkubectl get ns", blockRead},
		{"yaml", "kind: Pod", blockInert},
		{"", "$ python3 x.py\nOBJECT ROW", blockUnavailable},
		{"", "plain output", blockInert},
	} {
		if got := classifyBlock(tc.lang, strings.Split(tc.body, "\n")); got != tc.want {
			t.Errorf("classifyBlock(%q, %q) = %d, want %d", tc.lang, tc.body, got, tc.want)
		}
	}
}

// A cluster-agent section is replaced by its heading and the session note;
// its siblings survive, and the title section that contains it is not the
// one replaced.
func TestReplaceClusterAgentSections(t *testing.T) {
	src := strings.Split("# Title\n\nIntro.\n\n## Step 1\n\nKeep.\n\n## Step 2\n\nCall kanban_complete(x).\n\n### Detail\n\nMore.\n\n## Step 3\n\nKeep too.", "\n")
	got := strings.Join(replaceClusterAgentSections(src), "\n")
	for _, want := range []string{"Intro.", "## Step 1", "Keep.", "## Step 2", sessionReportNote, "## Step 3", "Keep too."} {
		if !strings.Contains(got, want) {
			t.Errorf("lost %q:\n%s", want, got)
		}
	}
	for _, gone := range []string{"kanban_complete", "### Detail", "More."} {
		if strings.Contains(got, gone) {
			t.Errorf("kept %q:\n%s", gone, got)
		}
	}
}

func TestPreambleSkipsHeadingsInsideCodeBlocks(t *testing.T) {
	got := insertPreamble(strings.Split("```bash\n# a comment\n```\n# Title\nBody", "\n"))
	i := slices.Index(got, sessionPreamble)
	if i < 1 || got[i-2] != "# Title" {
		t.Errorf("preamble placed at %d:\n%s", i, strings.Join(got, "\n"))
	}
}
