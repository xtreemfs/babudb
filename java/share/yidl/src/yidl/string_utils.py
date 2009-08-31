from cStringIO import StringIO


__all__ = ["indent", "pad", "lpad", "rpad"]


def indent( text, indent_spaces ):
    if len( text ) > 0 and len( indent_spaces ) > 0:
        return "\n".join( [indent_spaces + line.rstrip() for line in StringIO( text ).readlines()] ) + "\n"
    else:
        return text

def pad( left_padding, s, right_padding ):
    if len( s ) > 0:
        return left_padding + s + right_padding
    else:
        return s
        
def lpad( padding, s ):
    if len( s ) > 0:
        return padding + s
    else:
        return s
        
def rpad( s, padding ):
    if len( s ) > 0:
        return s + padding
    else:
        return s