---
kind: "work_record"
recordId: "35c6e3f4-66ea-439f-83c1-eadee32f3d22"
status: "approved"
scope: "planned_change"
workKind: "FEATURE"
origin: "internal"
completionMode: "verified"
createdAt: "2026-09-14T18:38:29.097Z"
provenance:
    sourcePlans:
        - "0f2ec347-aefb-4b20-8dd9-654edace668b"
---
# Discord ACP Form Elicitation

## Summary

Implemented official ACP v1 form elicitation for Discord-backed OpenAB sessions. Discord sessions now advertise form capability, handle reverse `elicitation/create` requests without outbound ID-collision loss, present bounded progressive forms with paging and text fallbacks, authorize only active verified human turn contributors, and return one terminal accept, decline, or cancel response. The change added subprocess ACP integration coverage, Discord behavior tests, and user documentation for pagination and fallback commands.

## Deviations from Plan

Implementation was reconciled from a prior execution tree and its uncommitted repair work before validation. Repository-wide `cargo fmt --check` remained unclean because of formatting diffs outside the feature path, and Windows cross-check could not run because the Windows target standard library was not installed.

## Future Planning Notes

Keep ACP reverse requests classified by `method` plus ID before outbound pending-response lookup, and keep form authority tied to the exact active dispatch batch. For future Discord form work, preserve the server-side presenter/coordinator split, paging for long Agent-controlled text, and text fallback for values that exceed Discord control limits.