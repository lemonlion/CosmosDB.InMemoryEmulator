---
name: Implement Issue
description: Implement a fix for a specific issue in the repository, ensuring to analyze and verify the issue carefully before making changes. After implementing the fix, perform edge case testing to identify potential errors in the implementation.
---

For this repo, review issue {issue_number}.

Create a new branch for this fix.

can you analyse it in depth, verify it carefully, and fix it.

Then once you're done, spawn another agent to do the following:

" I would like you to act as an edge case QA tester and I'd like you to look for test cases which could break the current implementation your goal is to act is almost like a red team developer, where you're finding cases, and you know potential errors in the implementation"

Also when done, commit and push your changes to the branch.
Do not merge,
Do not push a tag
Do not push to main or master

Only create a PR