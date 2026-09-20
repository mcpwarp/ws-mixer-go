# fetch-spec clones github.com/mcpwarp/ws-mixer-spec at the tag pinned in
# spec.pin into .spec/ (gitignored) -- the checkout wsmixer/specdir_test.go's
# specDir helper resolves fixtures from (.spec/spec/fixtures), and what CI's
# own git-clone-per-job step in .github/workflows/{go,conformance}.yml does
# too. No-op if .spec/ is already checked out at that pin. Mirrors
# ws-mixer-js's scripts/fetch-spec.mjs, same idea in that repo.
.PHONY: fetch-spec
fetch-spec:
	@pin=$$(cat spec.pin); \
	if [ -f .spec/.pin ] && [ "$$(cat .spec/.pin)" = "$$pin" ]; then \
		echo "fetch-spec: .spec/ already at $$pin"; \
	elif { [ -e .spec ] || [ -L .spec ]; } && [ ! -f .spec/.pin ]; then \
		echo "fetch-spec: .spec exists without a .pin stamp; remove it first" >&2; \
		exit 1; \
	else \
		rm -rf .spec; \
		echo "fetch-spec: cloning ws-mixer-spec@$$pin into .spec/"; \
		git clone --depth 1 --branch "$$pin" https://github.com/mcpwarp/ws-mixer-spec.git .spec; \
		echo "$$pin" > .spec/.pin; \
	fi
