package main

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"

	"sigs.k8s.io/yaml"
)

const (
	// frontmatterFence opens and closes a SKILL.md's YAML block.
	frontmatterFence = "---"
	// codeFence opens and closes a Markdown code block.
	codeFence = "```"
	// shellPrompt marks a command line inside an untagged block, as in a
	// worked example that shows a command and its output together.
	shellPrompt = "$ "
	// shellComment, lineContinuation, placeholderOpen and quoteChar are the
	// shell syntax the classifier reads: a comment line, a backslash that
	// joins the next line, a <placeholder> argument, and a quoted argument.
	shellComment     = "#"
	lineContinuation = "\\"
	placeholderOpen  = "<"
	quoteChar        = "\""
	// titlePrefix opens a skill's H1 title, which the preamble follows.
	titlePrefix = "# "
	// maxSkillNameLen and maxSkillDescriptionLen are the Agent Skills limits
	// on the two frontmatter fields Claude Code reads to list a skill.
	maxSkillNameLen        = 64
	maxSkillDescriptionLen = 1024
	// clusterAgentOnlyMarker names a tool only the cluster agent has. A
	// section that calls it is the cluster agent's reporting step, which a
	// session does not have; it is replaced by sessionReportNote.
	clusterAgentOnlyMarker = "kanban_"

	// proposeOnlyNote precedes a code block that changes a cluster or a
	// project. The session's broker refuses writes anyway; the note is what
	// keeps the model from trying, and from presenting the step as done.
	proposeOnlyNote = "> **Propose this to the operator; do not run it.** It changes a cluster or a project, and this session is read-only. Write it out as a proposal, or delegate it to `platform` if the person wants it done."
	// unavailableNote precedes a code block that runs a program the
	// session can't: Bash is limited to kubectl and gcloud, and the scripts
	// the skills name are not shipped.
	unavailableNote = "> **Not available in this session.** This runs a program this session can't run. With the cluster view, make the same reads with single `kubectl` or `gcloud` commands; otherwise say what you couldn't check."
	// pipeNote precedes a read-only block whose commands pipe into another
	// program, which the cluster view's Bash rule does not allow.
	pipeNote = "> Run each `kubectl` or `gcloud` command on its own, one per call, with no pipe, and read the output yourself."
	// sessionReportNote replaces a section that only the cluster agent can
	// carry out, keeping its heading so the skill's cross-references still land.
	sessionReportNote = "_In a session this step is your answer: give the root cause, the evidence that grounds it, and any proposed patch in your reply. The cluster agent's reporting tools don't exist here._"

	// shippedFileProse and shippedFileArg replace a reference to one of the
	// cluster agent's scripts, example files or settings, in prose and in a
	// command respectively. Only SKILL.md ships, and Claude Code hands the
	// model the skill's directory with its text, so a relative link left in
	// place would be an invitation to go looking for a file that isn't there.
	shippedFileProse = "the cluster agent's file (not in this session)"
	shippedFileArg   = "<file not in this session>"
	// filePathPattern is a relative scripts/ or assets/ path, an absolute
	// script path, or the cluster agent's settings file.
	filePathPattern = `(\./)?(assets|scripts)/[\w./-]+|/opt/data/[\w./-]+|SETTINGS\.md`

	// sessionPreamble follows each skill's title. It says, once, what the
	// per-block notes say locally, and covers what they can't mark: prose
	// that names files, tools and scripts this pod doesn't have.
	sessionPreamble = `> **Using this skill in a kube-agents session.** It was written for the cluster agent.
>
> - Run its ` + "`kubectl`" + ` and ` + "`gcloud`" + ` commands only when your system prompt gives you the read-only cluster view, one command per call. Without the view, use the steps to judge what the person showed you, or to say what ` + "`platform`" + ` should check when you delegate.
> - A step that changes a cluster or a project is a proposal, never something you run. Code blocks that do are marked; a step written as prose ("edit", "apply", "enable") is a proposal too.
> - Scripts, example files, settings files, git and tools the skill names (kanban, MCP servers, web search) are not in this session. Don't look for them; where a step describes a script's checks, make the same checks by reading the objects yourself.`
)

var (
	// fileLinkRE is a Markdown link to a relative scripts/ or assets/
	// path; filePathRE is filePathPattern on its own.
	fileLinkRE = regexp.MustCompile(`\[[^\]]*\]\((\./)?(assets|scripts)/[^)]*\)`)
	filePathRE = regexp.MustCompile(filePathPattern)
	// quotedFilePathRE is the same path as inline code in prose, replaced
	// with its backticks so the replacement reads as prose.
	quotedFilePathRE = regexp.MustCompile("`(" + filePathPattern + ")`")
	// skillNameRE is the Agent Skills name rule: lowercase letters, digits
	// and hyphens.
	skillNameRE = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)
	// headingRE matches an ATX heading and captures its level.
	headingRE = regexp.MustCompile(`^(#{1,6})\s`)
	// shellLangs are the info strings whose blocks hold commands.
	shellLangs = map[string]bool{"bash": true, "sh": true, "shell": true, "console": true, "zsh": true}
	// kubectlReadVerbs are the kubectl subcommands that only read. The
	// classifier is an allowlist: a verb not named here is treated as a
	// write, so a new skill step fails toward "propose" rather than "run".
	kubectlReadVerbs = map[string]bool{
		"get": true, "describe": true, "logs": true, "top": true, "explain": true,
		"api-resources": true, "api-versions": true, "version": true, "cluster-info": true,
		"events": true, "auth": true, "config": true,
	}
	// kubectlReadSubverbs narrows the verbs above that are command groups
	// with a writing member: `auth reconcile` writes RBAC, and `config`
	// rewrites the kubeconfig every read depends on.
	kubectlReadSubverbs = map[string]map[string]bool{
		"auth":   {"can-i": true, "whoami": true},
		"config": {"view": true, "current-context": true, "get-contexts": true, "get-clusters": true},
	}
	// kubectlValueFlags are the global flags that can precede the verb with
	// their value as a separate word. Any other flag in that position makes
	// its value read as the verb, which classifies as a write: fail closed.
	kubectlValueFlags = map[string]bool{
		"-n": true, "--namespace": true, "--context": true, "--kubeconfig": true,
		"--cluster": true, "--user": true,
	}
	// gcloudReadVerbs are the gcloud command-group leaves that only read.
	// get-credentials writes a local kubeconfig, which is how every other
	// read here reaches the cluster; the credential broker serves it.
	gcloudReadVerbs = map[string]bool{
		"describe": true, "list": true, "read": true, "get-credentials": true,
		"get-iam-policy": true, "get-server-config": true, "info": true, "version": true,
	}
)

// blockClass is what a code block asks the model to do, strictest last.
type blockClass int

const (
	blockInert       blockClass = iota // not commands: YAML, text, output
	blockRead                          // kubectl/gcloud reads, one per line
	blockPipe                          // reads, but piped into another program
	blockUnavailable                   // a program this image doesn't ship
	blockWrite                         // changes a cluster or a project
)

// skillFrontmatter is the part of a SKILL.md's YAML block Claude Code reads
// to list a skill. Anything else in the source block is dropped: fields such
// as allowed-tools or hooks would widen what the skill may do when invoked,
// and a skill with only these two fields is one the Skill tool runs without
// a permission rule naming it.
type skillFrontmatter struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

// transformSkill turns one cluster-agent SKILL.md into the session's copy:
// the same name and description, a session preamble after the title, a note
// before every code block that is not a plain read, and the cluster agent's
// reporting sections replaced by sessionReportNote.
func transformSkill(dirName string, src []byte) ([]byte, error) {
	fm, body, err := splitFrontmatter(src)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", dirName, err)
	}
	var meta skillFrontmatter
	if err := yaml.Unmarshal(fm, &meta); err != nil {
		return nil, fmt.Errorf("%s: frontmatter: %w", dirName, err)
	}
	if err := validateFrontmatter(dirName, meta); err != nil {
		return nil, err
	}
	// Written field by field so name leads, as in the source; Marshal
	// would sort the keys.
	var out []byte
	for _, field := range []map[string]string{{"name": meta.Name}, {"description": meta.Description}} {
		line, err := yaml.Marshal(field)
		if err != nil {
			return nil, fmt.Errorf("%s: frontmatter: %w", dirName, err)
		}
		out = append(out, line...)
	}

	lines := strings.Split(string(body), "\n")
	lines = replaceClusterAgentSections(lines)
	lines = annotateBlocks(lines)
	lines = replaceFileReferences(lines)
	lines = insertPreamble(lines)

	var b bytes.Buffer
	b.WriteString(frontmatterFence + "\n")
	b.Write(out)
	b.WriteString(frontmatterFence + "\n")
	b.WriteString(strings.Join(lines, "\n"))
	return b.Bytes(), nil
}

// validateFrontmatter holds the two fields to what Claude Code accepts. The
// skill's name is its directory name, so the two have to agree or the model
// would be told to load a skill by a name that does not resolve.
func validateFrontmatter(dirName string, meta skillFrontmatter) error {
	switch {
	case meta.Name != dirName:
		return fmt.Errorf("%s: frontmatter name %q does not match its directory", dirName, meta.Name)
	case len(meta.Name) > maxSkillNameLen || !skillNameRE.MatchString(meta.Name):
		return fmt.Errorf("%s: name %q is not lowercase letters, digits and hyphens of at most %d characters", dirName, meta.Name, maxSkillNameLen)
	case strings.TrimSpace(meta.Description) == "":
		return fmt.Errorf("%s: empty description; Claude Code lists a skill by it", dirName)
	case len(meta.Description) > maxSkillDescriptionLen:
		return fmt.Errorf("%s: description is %d characters, over %d", dirName, len(meta.Description), maxSkillDescriptionLen)
	}
	return nil
}

// splitFrontmatter separates the YAML block from the Markdown body.
func splitFrontmatter(src []byte) (fm, body []byte, err error) {
	s := string(src)
	if !strings.HasPrefix(s, frontmatterFence+"\n") {
		return nil, nil, fmt.Errorf("no frontmatter: the file does not open with %q", frontmatterFence)
	}
	rest := s[len(frontmatterFence)+1:]
	end := strings.Index(rest, "\n"+frontmatterFence+"\n")
	if end < 0 {
		return nil, nil, fmt.Errorf("frontmatter is not closed")
	}
	return []byte(rest[:end]), []byte(rest[end+len("\n"+frontmatterFence+"\n"):]), nil
}

// replaceClusterAgentSections swaps each section whose own text calls a
// cluster-agent tool, subsections included, for its heading and
// sessionReportNote. A section runs from its heading to the next heading at
// the same or a higher level.
func replaceClusterAgentSections(lines []string) []string {
	// own ends at the next heading of any level: the marker has to be in
	// the section's own text, or the title section, which spans the whole
	// file, would be the one replaced.
	type section struct{ start, own, end, level int }
	var sections []section
	inFence := false
	for i, l := range lines {
		if strings.HasPrefix(strings.TrimSpace(l), codeFence) {
			inFence = !inFence
			continue
		}
		if inFence {
			continue
		}
		if m := headingRE.FindStringSubmatch(l); m != nil {
			if n := len(sections); n > 0 {
				sections[n-1].own = i
			}
			sections = append(sections, section{start: i, own: len(lines), end: len(lines), level: len(m[1])})
		}
	}
	for i := range sections {
		for j := i + 1; j < len(sections); j++ {
			if sections[j].level <= sections[i].level {
				sections[i].end = sections[j].start
				break
			}
		}
	}

	var out []string
	next := 0
	for _, s := range sections {
		if s.start < next {
			continue // inside a section already replaced
		}
		if !strings.Contains(strings.Join(lines[s.start:s.own], "\n"), clusterAgentOnlyMarker) {
			continue
		}
		out = append(out, lines[next:s.start]...)
		out = append(out, lines[s.start], "", sessionReportNote, "")
		next = s.end
	}
	return append(out, lines[next:]...)
}

// annotateBlocks puts the strictest applicable note before each code block
// that is not inert or a plain read.
func annotateBlocks(lines []string) []string {
	var out []string
	for i := 0; i < len(lines); i++ {
		trimmed := strings.TrimSpace(lines[i])
		if !strings.HasPrefix(trimmed, codeFence) {
			out = append(out, lines[i])
			continue
		}
		indent := lines[i][:len(lines[i])-len(strings.TrimLeft(lines[i], " "))]
		lang := strings.TrimSpace(strings.TrimPrefix(trimmed, codeFence))
		end := i + 1
		for end < len(lines) && !strings.HasPrefix(strings.TrimSpace(lines[end]), codeFence) {
			end++
		}
		if note := noteFor(classifyBlock(lang, lines[i+1:min(end, len(lines))])); note != "" {
			out = append(out, indent+note, "")
		}
		stop := min(end+1, len(lines))
		out = append(out, lines[i:stop]...)
		i = stop - 1
	}
	return out
}

func noteFor(c blockClass) string {
	switch c {
	case blockWrite:
		return proposeOnlyNote
	case blockUnavailable:
		return unavailableNote
	case blockPipe:
		return pipeNote
	}
	return ""
}

// classifyBlock reads a code block's commands and returns the strictest
// class among them. A shell-tagged block is all commands; an untagged block
// counts only its "$ " lines, as in a worked example.
func classifyBlock(lang string, body []string) blockClass {
	shell := shellLangs[lang]
	if !shell && lang != "" {
		return blockInert
	}
	var cmds []string
	var cur strings.Builder
	for _, raw := range body {
		l := strings.TrimSpace(raw)
		if !shell {
			if !strings.HasPrefix(l, shellPrompt) {
				continue
			}
			l = strings.TrimPrefix(l, shellPrompt)
		}
		if cur.Len() == 0 && (l == "" || strings.HasPrefix(l, shellComment)) {
			continue
		}
		if strings.HasSuffix(l, lineContinuation) {
			cur.WriteString(strings.TrimSuffix(l, lineContinuation) + " ")
			continue
		}
		cur.WriteString(l)
		cmds = append(cmds, cur.String())
		cur.Reset()
	}
	if cur.Len() > 0 {
		cmds = append(cmds, cur.String())
	}

	worst := blockInert
	for _, c := range cmds {
		for i, seg := range splitShell(c) {
			class := classifyCommand(seg)
			if i > 0 && class != blockWrite {
				// A filter after a read (grep, jq) is the pipe the
				// cluster view's Bash rule refuses, not a missing tool.
				class = blockPipe
			}
			worst = max(worst, class)
		}
	}
	return worst
}

// splitShell splits a command line on pipes and command separators that sit
// outside quotes. The first segment is the command; a later one is either a
// pipe target or a second command, and both are classified.
func splitShell(line string) []string {
	var segs []string
	var cur strings.Builder
	var quote rune
	runes := []rune(line)
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		switch {
		case quote != 0:
			if r == quote {
				quote = 0
			}
			cur.WriteRune(r)
		case r == '\'' || r == '"':
			quote = r
			cur.WriteRune(r)
		case r == '|' || r == ';' || (r == '&' && i+1 < len(runes) && runes[i+1] == '&'):
			if r == '&' || (r == '|' && i+1 < len(runes) && runes[i+1] == '|') {
				i++
			}
			segs = append(segs, strings.TrimSpace(cur.String()))
			cur.Reset()
		default:
			cur.WriteRune(r)
		}
	}
	return append(segs, strings.TrimSpace(cur.String()))
}

// classifyCommand says what one command does. kubectl and gcloud are judged
// by verb against the read allowlists; anything else is a program the
// session image does not have.
func classifyCommand(cmd string) blockClass {
	fields := strings.Fields(cmd)
	if len(fields) == 0 {
		return blockInert
	}
	// A kubectl or gcloud behind a wrapper (env, timeout, xargs) is judged
	// as itself, so a write keeps its propose note; the wrapper is still a
	// program the session can't run.
	for i, f := range fields[1:] {
		if f == "kubectl" || f == "gcloud" {
			if classifyCommand(strings.Join(fields[i+1:], " ")) == blockWrite {
				return blockWrite
			}
			return blockUnavailable
		}
	}
	switch fields[0] {
	case "kubectl":
		args := fields[1:]
		for i := 0; i < len(args); i++ {
			f := args[i]
			if kubectlValueFlags[f] {
				i++ // the flag's value, not the verb
				continue
			}
			if strings.HasPrefix(f, "-") {
				continue
			}
			if !kubectlReadVerbs[f] {
				return blockWrite
			}
			if subs, grouped := kubectlReadSubverbs[f]; grouped && (i+1 >= len(args) || !subs[args[i+1]]) {
				return blockWrite
			}
			return blockRead
		}
		return blockWrite
	case "gcloud":
		for _, f := range fields[1:] {
			if strings.HasPrefix(f, "-") || strings.HasPrefix(f, placeholderOpen) || strings.HasPrefix(f, quoteChar) {
				break
			}
			if gcloudReadVerbs[f] {
				return blockRead
			}
		}
		return blockWrite
	}
	return blockUnavailable
}

// insertPreamble places sessionPreamble after the first H1, or at the top of
// the body if the skill has no title.
func insertPreamble(lines []string) []string {
	inFence := false
	for i, l := range lines {
		if strings.HasPrefix(strings.TrimSpace(l), codeFence) {
			inFence = !inFence
		}
		if !inFence && strings.HasPrefix(l, titlePrefix) {
			out := append([]string{}, lines[:i+1]...)
			out = append(out, "", sessionPreamble)
			return append(out, lines[i+1:]...)
		}
	}
	return append([]string{sessionPreamble, ""}, lines...)
}

// replaceFileReferences rewrites every reference to a file that is not
// shipped: a Markdown link or path in prose becomes shippedFileProse, and a
// path inside a code block becomes shippedFileArg.
func replaceFileReferences(lines []string) []string {
	out := make([]string, len(lines))
	inFence := false
	for i, l := range lines {
		if strings.HasPrefix(strings.TrimSpace(l), codeFence) {
			inFence = !inFence
			out[i] = l
			continue
		}
		if inFence {
			out[i] = filePathRE.ReplaceAllLiteralString(l, shippedFileArg)
			continue
		}
		l = fileLinkRE.ReplaceAllString(l, shippedFileProse)
		l = quotedFilePathRE.ReplaceAllLiteralString(l, shippedFileProse)
		out[i] = filePathRE.ReplaceAllLiteralString(l, shippedFileProse)
	}
	return out
}
