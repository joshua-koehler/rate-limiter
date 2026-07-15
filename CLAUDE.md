Commit atomically after each impl+test iteration - tests and the impl together.
Rust ecosystem with Tokio.
Anytime you ask me for a decision on something, and I answer, log my answer in the DECISIONS.md very succintly.
After each tier is complete, launch an adversarial review, focusing on criteria.md, and also paying attention to the original spec.md, I will pass ultimate judgment on the review, and you will respectively iterate or move on after that.
Be aware Rust compilation is slow, so save compilation till the end of a tier, but still commit along the way atomically, this is important for git history to show our work. this will sometimes result in corrections at the end of a tier to fix things, but it's worth the speed gains.
Be aware that other agents are working on this repo in parallel, don't let it spook or bewilder you.
