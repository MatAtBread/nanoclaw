---
name: switch-model
description: Switch the LLM provider/model mid-conversation while preserving full conversation context. Use when the user asks to change models, switch providers, or use a different AI.
allowed-tools: Bash, mcp__nanoclaw__send_message
---

# /switch-model — Change LLM Provider

Switch provider/model mid-conversation. Full conversation history is preserved and handed off to the new model.

## Available providers and models

| Provider arg | Model arg | Notes |
|---|---|---|
| `claude` | `claude-sonnet-4-6` (default) | Default — omit both args to reset |
| `claude` | `claude-opus-4-7` | Most capable Claude |
| `claude` | `claude-haiku-4-5-20251001` | Fastest/cheapest Claude |
| `deepseek` | `deepseek-v4-pro` | DeepSeek reasoning-focused |
| `deepseek` | `deepseek-v4-flash` | DeepSeek fast/cheap |

## Trigger

Run this skill when the user:
- Asks to "switch to DeepSeek / Opus / Haiku / etc"
- Says "change model", "use a cheaper model", "switch to a faster model"
- Types `/switch-model` with or without args

If the user is vague ("something cheaper", "a smarter model"), pick the best match from the table above and tell them what you chose.

## Step 1: Snapshot current conversation

Extract the live conversation transcript, stripping Claude-specific thinking blocks, and write it to the handoff file. The file persists in the mounted workspace across container restarts.

```bash
JSONL=$(find ~/.claude/projects -name "*.jsonl" 2>/dev/null | xargs ls -t 2>/dev/null | head -1)

node -e "
const fs = require('fs');
if (!process.argv[1] || !fs.existsSync(process.argv[1])) { console.log(''); process.exit(0); }
const lines = fs.readFileSync(process.argv[1], 'utf-8').split('\n');
const msgs = [];
for (const line of lines) {
  if (!line.trim()) continue;
  try {
    const e = JSON.parse(line);
    if (e.type === 'user' && e.message?.content) {
      const text = typeof e.message.content === 'string'
        ? e.message.content
        : e.message.content.map(c => c.text || '').join('');
      if (text.trim()) msgs.push('**User:** ' + text.trim());
    } else if (e.type === 'assistant' && e.message?.content) {
      const text = e.message.content
        .filter(c => c.type === 'text')
        .map(c => c.text)
        .join('');
      if (text.trim()) msgs.push('**Assistant:** ' + text.trim());
    }
  } catch {}
}
// Keep last 40 turns to avoid overwhelming context windows
const recent = msgs.slice(-40);
console.log(recent.join('\n\n'));
" "$JSONL" > /workspace/agent/context-handoff.md
```

If the JSONL is not found or the file is empty, write a brief summary of the conversation from memory instead:

```bash
cat > /workspace/agent/context-handoff.md << 'EOF'
[No live transcript available — brief summary written from memory]

<summary from memory here>
EOF
```

## Step 2: Update provider and model

```bash
ncl groups config update --provider <provider> --model <model>
```

Use the exact values from the table above. To reset to default Claude:
```bash
ncl groups config update --provider claude --model claude-sonnet-4-6
```

## Step 3: Restart with handoff message

```bash
ncl groups restart --message "You have just switched to <provider>/<model>. The previous conversation history is in /workspace/agent/context-handoff.md — read it immediately, then continue helping the user from where we left off. Delete the file after reading."
```

The current container will exit after this command. Do not send any further output — the restart message is what the new container will receive first.

## On startup after a switch

If you find `/workspace/agent/context-handoff.md` exists when you start:

1. Read it to restore conversation context
2. Delete it: `rm /workspace/agent/context-handoff.md`
3. Acknowledge to the user with a brief "Continuing as \*<model>\*..." confirmation, then proceed

Do not re-read the file later — delete it immediately after reading.
