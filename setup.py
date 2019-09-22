from setuptools import setup, find_packages

setup(
    name='userou',
    version='0.8',
    description='A tool for converting USR data from XML provided by Ministry of Justice of Ukraine',
    url='https://github.com/ap-Codkelden/userou',
    author='Renat Nasridinov',
    author_email='mavladi@gmail.com',
    install_requires=['lxml', 'requests'],
    license='MIT',
    packages=find_packages(),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Natural Language :: Ukrainian',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Operating System :: OS Independent',
        'Topic :: Database',
        'Topic :: Text Processing :: Markup :: XML',
        'Topic :: Utilities',
        'License :: MIT License'
    ],
    keywords='development data',
    project_urls={
    'Source': 'https://github.com/ap-Codkelden/userou/',
    },
    python_requires='>=3.6',
)