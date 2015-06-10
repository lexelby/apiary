"""Simple table formatting.

Each field is specified as a 2-tuple giving the justification and the content.  A row is a list of
such tuples.  An entire table is a list of rows.
"""


ALIGN_LEFT = "<"
ALIGN_CENTER = "^"
ALIGN_RIGHT = ">"


def format_table(rows):
    if not rows:
        return

    num_cols = max(len(row) for row in rows)

    # make all rows as long as the longest
    rows = [row + [(ALIGN_LEFT, "")] * (num_cols - len(row)) for row in rows]

    # calculate max column widths
    widths = [max(len(r[i][1]) for r in rows) for i in xrange(num_cols)]

    output = ""
    for row in rows:
        for i, (align, text) in enumerate(row):
            if i > 0:
                output += " "

            output += "{0:{1}{2}}".format(text, align, widths[i])
        output += "\n"

    return output
