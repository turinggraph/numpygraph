
.PHONY: doc

doc-upload: doc-build
	cp -r docs/build/html/* ../numpygraph.github.io; \
	cd ../numpygraph.github.io; \
	git add -A; \
	git commit -am autopush; \
	git push

doc-build:
	@echo doc build
	cd docs; make html
	@echo doc updating
	@echo doc updated


pypi-upload: pypi-build
	@echo pypi build
	@echo pypi updating
	python3 -m twine upload --repository pypi build/dist/*
	@echo pypi updated

pypi-build:
	# https://packaging.python.org/tutorials/packaging-projects/
	python3 setup.py sdist bdist_wheel
	rsync -av dist *.egg-info -t build
	rm -r dist *.egg-info 

upload-all: pypi-upload doc-upload


