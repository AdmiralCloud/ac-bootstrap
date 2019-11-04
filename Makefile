lint-check:
	./node_modules/.bin/eslint "packages/**/**.js"

lint-fix:
	./node_modules/.bin/eslint "packages/**/**.js" --fix

.PHONY: check
