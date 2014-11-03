#!/usr/bin/python

from copy import copy

fd = open('adam-core/src/test/resources/flag-values.sam', 'w')

sq_line='@SQ	SN:1	LN:249250621\n'
fd.write(sq_line)

read='read:0	0	1	1	60	75M	%s	%s	0	GTATAAGAGCAGCCTTATTCCTATTTATAATCAGGGTGAAACACCTGTGCCAATGCCAAGACAGGGGTGCCAAGA	*	NM:i:0	AS:i:75	XS:i:0'.split('\t')

for i in range(4096):

    # If read is marked as paired, exactly one of "first in template" or "second in template" must be set.
    if (i & 1 and i & 0x40 == i & 0x80):
        continue

    # Unpaired reads shouldn't have several flags set
    if (not (i & 1) and (i & 2 or i & 8 or i & 0x20 or i & 0x40 or i & 0x80)):
        continue

    # Unmapped reads shouldn't have "secondary alignment" or "supplementary alignment" flags set.
    if (i & 4 and (i & 0x100 or i & 0x800)):
        continue

    x = copy(read)
    x[0] = 'read:' + str(i)
    x[1] = str(i)
    x[4] = '0' if i & 4 else '60'
    x[6] = '=' if i & 1 else '*'
    x[7] = '1' if i & 1 else '0'
    fd.write('\t'.join(x) + '\n')

fd.close()
