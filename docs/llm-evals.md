# LLM Evaluations

`scripts/evaluate_llms.py` is a small eval harness for comparing model choices used by the Google Maps enrichment pipeline.

It covers two tasks:

- `semantic`: description, neighborhood, category/type tags, search tags, and vibe tags from cached enrichment evidence.
- `dom-repair`: JSON repair of captured Google Maps place-page DOM evidence from `gmaps-scraper`.

Eval outputs default to `.context/llm-evals/`, the repo's gitignored local runtime/scratch space. The same root is already used for scraper sessions and currency-rate cache data. Use `--output-root` if you want the run artifacts somewhere else. The harness is intended to be committed, but runs and responses are local artifacts unless you explicitly promote a fixture elsewhere.

## Model Profiles

Default model/provider data lives in [`scripts/llm_model_profiles.json`](../scripts/llm_model_profiles.json). Each profile includes:

- `provider`: `openai-compatible` or `anthropic`
- `model`: exact provider model string
- `api_key_env`: environment variable to read at call time
- `base_url`: OpenAI-compatible API base URL, when needed
- `request_options`: extra request JSON such as `response_format`
- `cost_per_1m`: directional input, cached input, and output token pricing for run cost estimates

The script rereads `.env` when each model call starts, so you can update keys without restarting the shell:

```bash
OPENAI_API_KEY=...
FIREWORKS_API_KEY=...
OPENROUTER_API_KEY=...
ANTHROPIC_API_KEY=...
```

Use `--profiles path/to/profiles.json` to layer local additions or overrides on top of the default JSON. The file can use the same shape:

```json
{
  "models": {
    "example-fireworks": {
      "provider": "openai-compatible",
      "model": "accounts/fireworks/models/example",
      "api_key_env": "FIREWORKS_API_KEY",
      "base_url": "https://api.fireworks.ai/inference/v1",
      "request_options": {
        "response_format": {
          "type": "json_object"
        }
      }
    }
  }
}
```

You can also use `openrouter:<model-id>` without adding a profile. For example, `openrouter:anthropic/claude-sonnet-4.5` maps to `OPENROUTER_API_KEY` and `https://openrouter.ai/api/v1`.

Unknown model ids use `OPENAI_API_KEY` by default. If you also pass `--base-url`, they use `LLM_API_KEY` by default; override that with `--api-key-env FIREWORKS_API_KEY` or define the model in a profile JSON when you want provider-specific credentials.

## Semantic Eval

Semantic evals should usually run from captured fixture cases. This keeps model comparisons stable and avoids spending time rebuilding the same sample set.

Capture a compact stratified suite from the local SQLite enrichment cache:

```bash
uv run python3 scripts/evaluate_llms.py \
  --run-name semantic-stratified-cases \
  semantic \
  --limit 24 \
  --include-noted \
  --fixture-suite stratified \
  --capture-only
```

Replay the same cases against model candidates:

```bash
uv run python3 scripts/evaluate_llms.py \
  --models gpt-5.5,gpt-5.4-mini,gpt-5.4-nano,gpt-4.1-mini,gpt-4.1-nano,kimi-k2p6-fireworks,kimi-k2p6-turbo-fireworks,kimi-k2p5-fireworks \
  --run-name semantic-fixture-run \
  semantic \
  --cases-file .context/llm-evals/semantic-stratified-cases/semantic-cases.jsonl \
  --force
```

The stratified suite labels cases by evidence shape, rough place type, geography, city, and non-English names. It is meant to include examples such as sparse evidence, handwritten notes, Google descriptions, search-result descriptions, review topics, review snippets, About sections, and multiple geographies. Prefer this over a large random run unless you are measuring broad corpus behavior.

Important semantic review points:

- Sparse evidence should normally produce `description: null`.
- Descriptions should be specific to the available evidence and avoid invented claims.
- Category/type tags should be accurate enough to power filtering.
- Vibe tags should be helpful for browsing without becoming generic filler.
- Compare candidate models against `gpt-5.5` as the oracle/baseline, but still manually inspect the baseline when the evidence is ambiguous.

## DOM Repair Eval

DOM repair evals can be captured once and replayed. Capture uses `llm_policy="always"` internally, so it records the LLM repair request even when the structural scraper parse would have succeeded without an LLM.

Capture repair cases:

```bash
uv run python3 scripts/evaluate_llms.py \
  --run-name dom-repair-cases \
  dom-repair \
  --limit 12 \
  --capture-only
```

Replay captured cases:

```bash
uv run python3 scripts/evaluate_llms.py \
  --models gpt-5.5,gpt-5.4-mini,kimi-k2p6-fireworks,kimi-k2p5-fireworks,deepseek-v4-pro-fireworks \
  --run-name dom-repair-fixture-run \
  dom-repair \
  --cases-file .context/llm-evals/dom-repair-cases/dom-repair-cases.jsonl \
  --force
```

Capture options such as `--collect-reviews`, `--collect-about`, `--guide`, `--place`, and `--headful` are only needed when creating cases. Replays use the saved request payloads and do not hit Google Maps.

## Outputs

Each run directory contains:

- `run.json`: command metadata, resolved profiles, and seed
- `<task>-cases.jsonl`: captured or replayed cases
- `<task>-results.jsonl`: per-case outputs from every model
- `responses/<task>/<model>/<case>.json`: cached raw model responses
- `judge-pack.md`: compact human-review pack with baseline output shown first

Timing data is only meaningful when the network is stable. If a run happens during bad connectivity, keep validity observations if the responses completed, but discard or ignore the timing metrics.

## Package Scripts

The package aliases are thin wrappers around the same harness:

```bash
bun run eval:llm -- --help
bun run eval:llm:semantic -- --help
bun run eval:llm:dom-repair -- --help
```

Pass global options such as `--models`, `--profiles`, and `--run-name` before the subcommand when using `uv run python3 scripts/evaluate_llms.py` directly or the `bun run eval:llm -- ...` alias. The task-specific package aliases already include the subcommand, so the same global options are accepted after the alias:

```bash
bun run eval:llm:semantic -- --models gpt-5.5,kimi-k2p6-fireworks --cases-file .context/llm-evals/semantic-stratified-cases/semantic-cases.jsonl
```
