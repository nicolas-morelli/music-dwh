from setuptools import setup, find_packages

setup(
    name='music_dwh',
    version='0.1.0',
    author='Domingo Morelli',
    author_email='nicomorelli47@gmail.com',
    description='',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/nicolas-morelli/music-dwh',
    packages=find_packages(),
    install_requires=[],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.12',
)