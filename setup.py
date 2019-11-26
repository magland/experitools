import setuptools

pkg_name = "experitools"

setuptools.setup(
    name=pkg_name,
    version="0.1.0",
    author="Jeremy Magland",
    author_email="jmagland@flatironinstitute.org",
    description="Experimental tools by J. Magland",
    packages=setuptools.find_packages(),
    scripts=[],
    install_requires=[
        'kachery'
    ],
    classifiers=(
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    )
)
