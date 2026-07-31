<!-- Paste this at the very top of the project README. It reads the badge JSON that the nightly
     chaos job publishes to the `badges` branch, rendered live by shields.io's endpoint API. -->

[![chaos](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/OWNER/REPO/badges/chaos-badge.json)](../../actions/workflows/nightly-chaos.yml)

<!-- Replace OWNER/REPO with the GitHub owner and repository name. The badge shows:
       chaos: passing   (bright green)  — last nightly run proved all invariants held
       chaos: failing   (red)           — an invariant was violated; click through to the run
     The badge and the CI status are always consistent: the verification script fails the job on
     any violation, so a red build cannot sit under a green badge. -->
