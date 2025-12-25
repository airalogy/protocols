# Tests

The protocols in this folder are primarily used as test fixtures to validate the core syntax and functionality of Airalogy Protocol.

## Fixtures

- `test_markdown_conversion`: Convert `.docx` and `.pdf` uploads to Markdown in separate sections (manual assigners); uses `markitdown` backend (not user-configurable).
- `test_multi_level_assigner`: Multi-level assigner chains (auto/manual/auto_first) plus a slow assigner using `time.sleep()` to simulate long-running assignments.
