from distutils.core import setup

try:
    import greenlet
except ImportError:
    raise Exception('greenlet missing. You can get it at http://pypi.python.org/pypi/greenlet')

setup(name='naglfar',
    description='Asynchronous IO library for python using greenlet based coroutines',
    keywords='greenlet event async coroutine channel',
    url='http://github.com/gorset/naglfar',
    license='BSD',
    author='Erik Gorset',
    author_email='erik@gorset.no',
    packages=['naglfar']
)
