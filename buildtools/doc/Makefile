all: HowToRelease.html windowsbuild.html HowToRelease.pdf windowsbuild.pdf

HowToRelease.html: HowToRelease.rst
	rst2html HowToRelease.rst > HowToRelease.html

windowsbuild.html: windowsbuild.rst
	rst2html windowsbuild.rst > windowsbuild.html

HowToRelease.pdf: HowToRelease.rst
	rst2pdf HowToRelease.rst

windowsbuild.pdf: windowsbuild.rst
	rst2pdf windowsbuild.rst
