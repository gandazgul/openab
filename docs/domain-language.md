# Domain Language

Use these terms consistently in code comments, docs, plans, and review notes.

## Elicitation

An **Elicitation** is an official ACP Agent-to-Client request that asks a verified user for more information while an Agent Runtime is working. In OpenAB, the downstream Agent Runtime sends `elicitation/create` over the existing stdio JSON-RPC connection.

Do not use **permission prompt** as a synonym. Tool permission requests use `session/request_permission` and have a different policy path.

## Form Elicitation

A **Form Elicitation** is an ACP `elicitation/create` request with `mode: "form"`. It collects non-sensitive structured input from verified Platform users and returns one of:

- `accept` with optional structured `content`;
- `decline`;
- `cancel`.

In the Discord adapter, OpenAB presents form elicitation with Discord controls when safe and with direct reply text fallback when controls cannot safely represent a field. A Discord modal is one possible control, not the protocol concept.

Form elicitation must not request secrets such as passwords, API keys, access tokens, private keys, recovery codes, or payment credentials.
