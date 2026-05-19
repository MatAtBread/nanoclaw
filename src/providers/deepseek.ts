import { registerProviderContainerConfig } from './provider-container-registry.js';

registerProviderContainerConfig('deepseek', () => ({
  env: {
    ANTHROPIC_BASE_URL: 'https://api.deepseek.com/anthropic',
    ANTHROPIC_AUTH_TOKEN: 'placeholder',
  },
}));
