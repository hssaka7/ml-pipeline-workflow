from setuptools import setup, find_packages

setup(
    name='mltool',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'graphviz',
        'matplotlib',
        'networkx',
        'numpy',
        'pandas',
        'python-dotenv',
        'PyYAML',
        'scikit-learn'
        # List your dependencies here
    ],
    entry_points={
        'console_scripts': [
            'mltool=core.cli:start',
        ],
    },
)