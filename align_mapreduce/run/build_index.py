"""
" ex building cmd: /home/dane2522/programs/bowtie2-2.2.9/bowtie2-build -f tmp_ref/genome.fa_bowtie2/W_C2T.fa tmp_ref/genome.fa_bowtie2/W_C2T
"  => bowtie2-build -f <ref.fa> <prefix>

"""
import string
import argparse
import sys
import os
sys.path.append( os.path.join(os.path.dirname(__file__), "..", "utils") )
import utils
import subprocess
from multiprocessing import Process 


conv_way = ["W_C2T", "C_C2T", "W_G2A", "C_G2A"]


###
# 1. read fasta file
# 2. Convert in four way
# 3. run command
###
def build_index( args):
  i_file = args.input

  utils.logging("[INFO] Start downloading reference file.", args)
  tempbase = utils.gen_file()
  utils.mkdir(tempbase)
  reffile = os.path.join( tempbase, "raw.fa" )
  utils.read_hdfs( i_file, reffile )

  tempfiles = [ open(os.path.join(tempbase, "%s.fa"%m), 'w') for m in conv_way ]


  utils.logging("[INFO] Start transforming reference file.", args)
  # read ref
  for chrid, seq in utils.read_fasta( reffile):
    for i, method in enumerate(conv_way):
      (strand, a_from, a_to) = (method[0], method[2], method[4])

      if strand == "W":
        tempfiles[i].write(">%s\n%s\n" % (chrid, seq.translate( utils.make_trans_with(strand, a_from, a_to))))
      else:
        tempfiles[i].write(">%s\n%s\n" % (chrid, seq.translate( utils.make_trans_with(strand, a_from, a_to))[::-1]))


  # close all files
  for i, method in enumerate(conv_way):
    tempfiles[i].close()

  utils.logging("[INFO] Start launching bowtie2-build.", args)
  # run bowtie jobs
  procs = []

  utils.mkdir( os.path.join(tempbase, "index") )
  for i, method in enumerate(conv_way):
    out_pref = os.path.join(tempbase, "index", method)
    build_log = out_pref + ".build.log"

    proc = Process(target=call_bowtie, args=(tempfiles[i].name, out_pref, build_log,))
    procs.append( proc )
    proc.start()

  for proc in procs:
    proc.join()


  utils.logging("[INFO] Start uploading index file.", args)
  # move to hdfs
  utils.copy_to_hdfs(tempbase, args.output, remove_original=True)
  # utils.copy_to_hdfs(tempbase, args.output, remove_original=False)





# input: method
# return: processed_file_name
def call_bowtie(input_ref, out_pref, log):
  query = ["bowtie2-build", "-f", input_ref, out_pref]
  utils.logging("[INFO] bowtie2-build is called as: %s" % (" ".join(query)), args)
  # make index with transformed genome
  f = open(log, 'w')
  p = subprocess.Popen(query, stdout=f)
  p.communicate()






if __name__ == "__main__":
  parser = argparse.ArgumentParser()

  ### args
  parser.add_argument("--input", type=str, default="", help="input reference file")
  parser.add_argument("--output", type=str, default="", help="output reference path")
  parser.add_argument("--log", type=str, default="", help="log path")

  parser.parse_args()
  args, unparsed = parser.parse_known_args()

  utils.logging("[INFO] Building index of <%s> is launched." % (args.input), args)
  for k, v in vars(args).iteritems():
    utils.logging("[INFO] Loggin arguments: %s : %s." % (str(k), str(v)), args)

  utils.mkdir( os.path.dirname(args.log) )

  if args.input == "" or args.output == "":
    utils.logging("[ERROR] Input and Output File must be correctly set.", args)
    sys.exit()

  build_index( args)