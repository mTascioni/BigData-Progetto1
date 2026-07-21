// Client per il router di Hugging Face Inference Providers (API OpenAI-
// compatibile). Stesso pattern usato in un altro progetto (chat/completions
// con Bearer token), adattato a ES modules.
export default class LlmService {
  constructor(apiKey, model) {
    this.apiKey = apiKey;
    this.model = model;
  }

  async getResponse(messages, { maxTokens = 800, temperature = 0.1 } = {}) {
    const response = await fetch("https://router.huggingface.co/v1/chat/completions", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${this.apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        messages,
        model: this.model,
        max_tokens: maxTokens,
        temperature,
      }),
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Hugging Face API error: ${response.status} - ${errorText}`);
    }

    const data = await response.json();
    if (!data.choices || !data.choices[0] || !data.choices[0].message) {
      throw new Error(`Struttura di risposta inattesa da Hugging Face: ${JSON.stringify(data)}`);
    }

    return data.choices[0].message.content;
  }
}
