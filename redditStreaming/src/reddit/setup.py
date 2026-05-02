import setuptools
 
setuptools.setup(
    name="reddit",
    version="0.1.0",
    author="steven hurwitt",
    author_email="stevenhurwitt@gmail.com",
    description="reddit api kafka & pyspark",
    long_description="kafka producer and spark streaming application for multiple subreddits",
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)