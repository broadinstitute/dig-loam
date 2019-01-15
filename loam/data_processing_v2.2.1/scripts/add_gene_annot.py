from optparse import OptionParser
import sys
import bisect
import re

usage = "usage: %prog [options]"
parser = OptionParser(usage)
parser.add_option("","--gene-file",action="append")
parser.add_option("","--gene-file-header",default=0, type="int")
parser.add_option("","--gene-file-comment",default=None)
parser.add_option("","--gene-file-id-col",type="int",default=1)
parser.add_option("","--gene-file-chr-col",type="int",default=None)
parser.add_option("","--gene-file-start-col",type="int",default=None)
parser.add_option("","--gene-file-end-col",type="int",default=None)
parser.add_option("","--gene-file-num-ids",type="int",default=1)
parser.add_option("","--gene-file-num-ids-print",type="int",default=None)
parser.add_option("","--gene-id-join-char",type="string",default=":")
parser.add_option("","--gene-add-flank",type="int",default=0)
parser.add_option("","--outside-name",default="Outside")
parser.add_option("","--keep-outside",action="store_true",default=False)
parser.add_option("","--no-outside",action="store_true",default=False)
parser.add_option("","--locus-col",default=None, type="int")
parser.add_option("","--chr-col",default=None, type="int")
parser.add_option("","--pos-col",default=None, type="int")
parser.add_option("","--pos2-col",default=None, type="int")
parser.add_option("","--break-multiple-col",default=None, type="int")
parser.add_option("","--in-delim",default=None)
parser.add_option("","--out-delim",default=None)
parser.add_option("","--print-multiple",action="store_true",default=False)
parser.add_option("","--header",default=0, type="int")
parser.add_option("","--comment",default="#")

(options, args) = parser.parse_args()
gene_files = options.gene_file
gene_file_num_ids = options.gene_file_num_ids
gene_file_id_col = options.gene_file_id_col - 1

if options.gene_file_chr_col is not None:
    gene_file_chr_col = options.gene_file_chr_col - 1
else:
    gene_file_chr_col = gene_file_num_ids + gene_file_id_col

if options.gene_file_start_col is not None:
    gene_file_start_col = options.gene_file_start_col - 1
else:
    gene_file_start_col = gene_file_num_ids + gene_file_id_col + 1

if options.gene_file_end_col is not None:
    gene_file_end_col = options.gene_file_end_col - 1
else:
    gene_file_end_col = gene_file_num_ids + gene_file_id_col + 2

num_gene_file_header = options.gene_file_header
gene_file_num_ids_print = options.gene_file_num_ids_print
if gene_file_num_ids_print is None or gene_file_num_ids_print > gene_file_num_ids:
    gene_file_num_ids_print = gene_file_num_ids

keep_outside = options.keep_outside
no_outside = options.no_outside
locus_col = options.locus_col
chr_col = options.chr_col
pos_col = options.pos_col
pos2_col = options.pos2_col
break_multiple_col = options.break_multiple_col
in_delim = options.in_delim
out_delim = options.out_delim
outside_name = options.outside_name
num_header = options.header
comment = options.comment
print_multiple = options.print_multiple

use_interval_tree = True

unspacify = False
if out_delim is None:
    if in_delim is None:
        unspacify = True
        out_delim = ' '
    else:
        out_delim = in_delim

if out_delim == '\\t':
    out_delim = "\t"


if not gene_files:
    raise Exception("Need gene file")

if locus_col is None and (chr_col is None or pos_col is None):
    raise Exception("Need 1-based locus col or 1-based chr and pos cols")

if locus_col is not None:
    locus_col -= 1 
if pos_col is not None:
    pos_col -= 1 
if pos2_col is not None:
    pos2_col -= 1 
if chr_col is not None:
    chr_col -= 1 

#right now using this only for case of print_multiple
#only to ensure backward compatibility
#once it seems to work we could use it exclusively
class IntervalTree(object):
    __slots__ = ('intervals', 'left', 'right', 'center')
    def __init__(self, intervals, depth=16, minbucket=96, _extent=None, maxbucket=4096):
        depth -= 1
        if (depth == 0 or len(intervals) < minbucket) and len(intervals) > maxbucket:
            self.intervals = intervals
            self.left = self.right = None
            return
        
        left, right = _extent or (min(i[0] for i in intervals), max(i[1] for i in intervals))
        center = (left + right) / 2.0

        self.intervals = []
        lefts, rights  = [], []

        for interval in intervals:
            if interval[1] < center:
                lefts.append(interval)
            elif interval[0] > center:
                rights.append(interval)
            else: # overlapping.
                self.intervals.append(interval)
                
        self.left   = lefts  and IntervalTree(lefts,  depth, minbucket, (left,  center)) or None
        self.right  = rights and IntervalTree(rights, depth, minbucket, (center, right)) or None
        self.center = center

    def find(self, start, stop):
        """find all elements between (or overlapping) start and stop"""
        overlapping = [i for i in self.intervals if i[1] >= start and i[0] <= stop]

        if self.left and start <= self.center:
            overlapping += self.left.find(start, stop)

        if self.right and stop >= self.center:
            overlapping += self.right.find(start, stop)

        return list(set(overlapping))

#store the current values of the gene we see
cur_gene = None
cur_chr = None
cur_start = None
cur_end = None

chr_to_start = dict()
chr_interval_to_gene = dict()
chr_start_to_end = dict()

intervals = {}

for gene_file in gene_files:

    gene_fh = open(gene_file, 'r')

    num = 0

    for line in gene_fh:

        line = line.strip()
        if options.gene_file_comment is not None and re.match(options.gene_file_comment, line):
            continue

        num = num + 1
        if num <= num_gene_file_header:
            continue


        cols = line.split()
        if len(cols) < gene_file_num_ids + 3:
            raise Exception("Gene file must be white-space delimited and have at least %s columns: gene..gene,chr,start,end" % (gene_file_num_ids + 3))

        gene_cols = cols[gene_file_id_col:gene_file_id_col + gene_file_num_ids]
        if unspacify:
             gene_cols = list(map(lambda x: re.sub(' ', '_', x), gene_cols))

        gene = options.gene_id_join_char.join(gene_cols[:gene_file_num_ids_print])

        chr = cols[gene_file_chr_col]
        if (chr[:3] == 'chr'):
            chr = chr[3:]

        try:
            start = int(cols[gene_file_start_col]) - options.gene_add_flank
            end = int(cols[gene_file_end_col]) + options.gene_add_flank
        except ValueError:
            raise Exception("Gene start and end values must be integers for %s: %s, %s" % (line, cols[gene_file_start_col], cols[gene_file_end_col]))

        if end < start:
            raise Exception("Gene end must be after gene start for %s" % line)

        if chr not in chr_to_start:
            if use_interval_tree:
                intervals[chr] = []
            chr_to_start[chr] = []
            chr_interval_to_gene[chr] = dict()
            chr_start_to_end[chr] = dict()

        chr_to_start[chr].append(start)

        if (start, end) not in chr_interval_to_gene[chr]:
            chr_interval_to_gene[chr][(start, end)] = []

        chr_interval_to_gene[chr][(start, end)].append(gene) 
        chr_start_to_end[chr][start] = end

        if use_interval_tree:
            intervals[chr].append((start, end))

    gene_fh.close()


#sort them
for chr in chr_to_start:
    chr_to_start[chr].sort()

if use_interval_tree:
    interval_tree = {}
    for int_chr in intervals:
        interval_tree[int_chr] = IntervalTree(intervals[int_chr])

num = 0
for line in sys.stdin:

    line = line.strip()

    if re.match(comment, line):
        print(line)
        continue

    num = num + 1
    if num <= num_header:
        if gene_file_num_ids_print > 0:
            print("GENE%s%s" % (out_delim, line))
        else:
            print(line)
        continue

    if in_delim is None:
        cols = line.split()
    else:
        cols = line.split(in_delim)
    
    chr = None
    pos = None
    if chr_col is not None:
        if chr_col >= len(cols):
            raise Exception("Error: Chr col %d out of bounds (%s) for %s" % (chr_col, len(cols), line))
        chr = cols[chr_col]

    if pos_col is not None:
        if pos_col >= len(cols):
            raise Exception("Error: Pos col %d out of bounds for %s" % (pos_col, line))
        pos = cols[pos_col]

    if pos2_col is not None:
        if pos2_col >= len(cols):
            raise Exception("Error: Pos 2 col %d out of bounds for %s" % (pos2_col, line))
        pos2 = cols[pos2_col]

    if locus_col is not None:
        if locus_col >= len(cols):
            raise Exception("Error: Locus col %s out of bounds for %s" % (locus_col, line))
        locus = cols[locus_col]
        locus = locus.split(':')
        if len(locus) != 2:
            raise Exception("Error: Couldn't parse locus %s (must be of form chr:position) for %s" % (locus, line))
        chr = locus[0]
        pos = locus[1]

    if chr is None or pos is None:
        raise Exception("Error: Didn't get chr and pos for %s" % line)

    if (chr[:3] == 'chr'):
        chr = chr[3:]

    try:
        m = re.match('([0-9]+)(\.[\.]+|-)([0-9]+)', pos)
        if m:
            pos = (int(m.group(1)) + int(m.group(3)))/2

        pos = int(pos)
    except ValueError:
        raise Exception ("Error: pos %s not an integer for %s" % (pos, line))

    found_gene = False

    genes = set()
    if cur_chr is None or not chr == cur_chr or pos < cur_start or pos > cur_end:
        #need to update
        if chr in chr_to_start:
            chr_starts = chr_to_start[chr]
            cur_results = set()

            #always use interval tree
            if use_interval_tree:
                cur_pos2 = pos
                if pos2_col is not None:
                    cur_pos2 = pos2
                cur_results = interval_tree[chr].find(pos, cur_pos2)
            else:
                ind = bisect.bisect_right(chr_starts, pos) - 1
                if ind < 0:
                    ind = 0
                cur_start = chr_starts[ind]
                cur_end = chr_start_to_end[chr][cur_start]

                #for overlapping genes, scan to the left if need be
                while pos > cur_end and ind > 0:
                    ind -= 1
                    cur_start = chr_starts[ind]
                    cur_end = chr_start_to_end[chr][cur_start]

                cur_results.add((cur_start, cur_end))
                
            cur_results = [x for x in cur_results if pos >= x[0] and pos <= x[1]]
            minimum = None
            min_result = None
            for cur_result in cur_results:
                if print_multiple or len(cur_result) == 1:
                    for cur_gene in chr_interval_to_gene[chr][cur_result]:
                        genes.add(cur_gene)
                else:
                    dist = min(abs(pos - (cur_result[0] + options.gene_add_flank)), abs(pos - (cur_result[1] - options.gene_add_flank)))
                    if minimum is None or dist < minimum:
                        minimum = dist
                        chr_interval_to_gene[chr][cur_result]
                        if chr_interval_to_gene[chr][cur_result]:
                            min_result = chr_interval_to_gene[chr][cur_result][0]
            if not print_multiple and min_result is not None:
                genes.add(min_result)
        
    if len(genes) == 0:
        if no_outside:
            raise Exception ("Error: outside for %s:%s" % (chr, pos))
        elif not keep_outside:
            continue
        else:
            genes = set([outside_name])

    for gene in genes:
        if gene_file_num_ids_print == 0:
            print(line)
            break
        else:
            print("%s%s%s" % (gene, out_delim, line))

