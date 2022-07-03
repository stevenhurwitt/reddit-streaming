import setuptools
 
setuptools.setup(
    name="secrets",
    version="1.0.0",
    author="steven hurwitt",
    author_email="stevenhurwitt@gmail.com",
    description="glue job spark serverless",
    long_description="glue job spark serverless",
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)
