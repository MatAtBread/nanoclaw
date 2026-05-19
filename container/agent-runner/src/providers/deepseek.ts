import { registerProvider } from './provider-registry.js';
import { ClaudeProvider } from './claude.js';

// DeepSeek uses an Anthropic-compatible API endpoint. The host-side provider
// plugin sets ANTHROPIC_BASE_URL to https://api.deepseek.com/anthropic and
// ANTHROPIC_AUTH_TOKEN=placeholder; OneCLI injects the real DeepSeek key.
registerProvider('deepseek', (opts) => new ClaudeProvider(opts));
