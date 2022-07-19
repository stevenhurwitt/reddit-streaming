import setuptools
 
setuptools.setup(
    name="target",
    version="1.0.0",
    author="steven hurwitt",
    author_email="stevenhurwitt@gmail.com",
    description="target uberjar & .whl files",
    long_description="move target build files to s3.",
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)
