#-------------------------------------------------------------------
#-*- coding: iso-8859-1 -*-
#-------------------------------------------------------------------

class _Getch:
    """Gets a single character from standard input.  Does not echo to the
screen."""
    def __init__(self):
        try:
            self.impl = _GetchWindows()
        except ImportError:
            self.impl = _GetchUnix()

    def __call__(self): return self.impl()


class _GetchUnix:
    def __init__(self):
        import tty, sys

    def __call__(self):
        import sys, tty, termios
        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        try:
            tty.setraw(sys.stdin.fileno())
            ch = sys.stdin.read(1)
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        return ch


#msvcrt.kbhit()
class _GetchWindows:
    def __init__(self):
        import msvcrt

    def __call__(self):
        import msvcrt
        return msvcrt.getch()

class _GetchNoBloqueante:
    """Gets a single character from standard input.  Does not echo to the
screen. Does not wait for input"""
    def __init__(self):
        try:
            self.impl = _GetchNoBloqueanteWindows()
        except ImportError:
            self.impl = _GetchNoBloqueanteUnix()

    def __call__(self): return self.impl()


class _GetchNoBloqueanteUnix:
		def __init__(self):
			import tty, sys
		def hayTecla():
			import select
			return select.select([sys.stdin], [], [], 0) == ([sys.stdin], [], [])
		def __call__(self):
			import sys, tty, termios
			try:
				tty.setcbreak(sys.stdin.fileno())
				if self.hayTecla():
					c = sys.stdin.read(1)
					return c
				else:
					return None
			finally:
				termios.tcsetattr(sys.stdin, termios.TCSADRAIN, old_settings)


class _GetchNoBloqueanteWindows:
	def __init__(self):
		import msvcrt

	def __call__(self):
		import msvcrt
		if msvcrt.kbhit():
			return msvcrt.getch()
		else:
			return None

getch = _Getch()
getchNoBloqueante = _GetchNoBloqueante()

def main():
	print "Pulse una tecla"
	tecla=getch()
	print "Tecla pulsada: ", tecla

if __name__ == "__main__":
	main()
