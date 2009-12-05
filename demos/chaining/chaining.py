"""The go chaining example

More information can be found at:
http://erik.gorset.no/2009/11/go-chaining-example-written-in-python.html
"""

import naglfar

def main(n):
    def runner(left, right):
        left.write(right.read() + 1)

    leftmost = left = naglfar.Channel()
    for i in xrange(n):
        right = naglfar.Channel()
        naglfar.go(runner, left, right)
        left = right

    right.write(0)
    print leftmost.read()

if __name__ == "__main__":
    import sys
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 100000
    print 'Chaining with %s coroutines' % n
    main(n)
