"""Simple table formatting.

Each field is specified as a 2-tuple giving the justification and the content.  A row is a list of
such tuples.  An entire table is a list of rows.
"""


ALIGN_LEFT = "<"
ALIGN_CENTER = "^"
ALIGN_RIGHT = ">"


def format_table(rows):
    widths = [max(len(r[i][1]) for r in rows) for i in xrange(len(rows[0]))]

    output = ""
    for row in rows:
        for i, (align, text) in enumerate(row):
            if i > 0:
                output += " "

            output += "{0:{1}{2}}".format(text, align, widths[i])
        output += "\n"

    return output
