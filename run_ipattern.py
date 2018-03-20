#! /usr/bin/python

import os, re, sys 
import ipn_util
from shutil import copy

'''
Run ipattern script.
Change:
	09/17/2009
	* Add error checking, check report/qerr files.
	  NOTE "Rerr" files are not used.
	* destination directory, if NOT given, will be generated automatically
	  based on the input of the directory.
'''

# useful directories
# these generally are constant, unless something weird happens, there 
# is no need to change them
ipn_path    = ipn_util.get_ipn_path ( )
merge_calls = os.path.join (ipn_path, 'merge_call_files_chg_names.py')

# commonly used commands
parser    = './iPatternRawReportParserAndFilter.pl'
ip_func   = './iPattern.Func.R'
ip_run    = './iPattern.Runner.R'

def write_job_file (intfile):
    qfile=intfile +'.qsub'

    #workdir=os.getcwd()
    workdir=tmpdir
    R_interpreter=ipn_util.get_R_interpreter()

    outf = open ( qfile , 'w')
    print >> outf, "#! /bin/bash"
    print >> outf, "#PBS -N j%s" % (intfile)
    print >> outf, "#PBS -e %s/%s.qerr" % (workdir, intfile)
    print >> outf, "#PBS -o %s/%s.qout" % (workdir, intfile)
    print >> outf, "#PBS -l h_vmem=8G"
    print >> outf, "#PBS -l walltime=6:00:00" #Changed this shit to PBS from $

    #print >> outf, "source ~/.bashrc"
    print >> outf, "tmpdir=${TMPDIR:-%s}" % (workdir)
    print >> outf, "job_id=${JOB_ID:-$$}"

    print >> outf, "thedir=$tmpdir/%s" % ("$job_id")
    print >> outf, "mkdir $thedir"
    print >> outf, "cd $thedir"
    print >> outf, "echo $tmpdir"
    print >> outf, "echo `hostname`"
    print >> outf, "echo $PATH"
    print >> outf, "echo `pwd`"

    print >> outf, "ln -s %s/ipattern.conf ." % (workdir)
    print >> outf, "ln -s %s/%s ." % (workdir, intfile)
    print >> outf, "ln -s %s/*.R ." % (workdir)
    print >> outf, "ln -s %s/known.cnvr.txt ." % (workdir)

    if ( os.path.exists('sample.info.txt') ):
        print >> outf, "ln -s %s/sample.info.txt ." % (workdir)

    print >> outf, "%s --no-restore --no-save --no-readline ipattern.conf %s < iPattern.Runner.R > /dev/null 2> %s/%s.$job_id.Rerr" % (R_interpreter, intfile, workdir, intfile)

    print >> outf, "rm ipattern.conf %s *.R known.cnvr.txt" % (intfile)
    print >> outf

    print >> outf, "cp %s.ipttn.txt %s" % (intfile, workdir)

    print >> outf, "cd .."
    print >> outf, "tar -zcf %s.ipttn.tar.gz %s" % (intfile, "$job_id")
    print >> outf

    print >> outf, "cp %s.ipttn.tar.gz %s" % (intfile, workdir)
    print >> outf, "rm -rf %s" % ("$thedir")
    print >> outf

    print >> outf, "cd %s" % (workdir)
    print >> outf

    print >> outf, "# reformat the file"
    print >> outf, "%s < ./%s.ipttn.txt > ./%s.ipttn.txt.report" % (parser, intfile, intfile)

# indicate the running job is finished
    print >> outf, "touch .tmp/.%s.qsub" % (intfile)

    outf.close()

    return qfile
# --------------------------------------------------------------------------- |

def run_jobs (dirname='.'):
    import ipn_qsub
    intfiles = ipn_util.myglob (dirname, 'int$')

    qlist=[]
    for a in intfiles:
	qfile=write_job_file (a)
	qlist.append (qfile)

    if not noqsub:
    	ret=ipn_qsub.run_wait_poll(qlist)

	if ( ret is not None ):
	    print >> sys.stderr, 'Running iPattern has problem.'
    else:
	for chromfile in qlist:
	    cmd_ele=["bash", chromfile]
	    print >> sys.stderr, ' '.join (cmd_ele)

	    ret, out, err=ipn_qsub.run_cmd_on_node ( cmd_ele )

# -------------------------------------------------------------------------- |
    

def check_report_file ( fname ):
    return os.path.exists (fname)
# ------------------------------------------------------------------------- |


# if there exist qerr/qout/Rerr files, remove them
def cleanup ( dirname = '.' ):
    print >> sys.stderr, 'from:', os.getcwd()
    
    files=os.listdir(dirname)
    for nm in files:
	print >> sys.stderr, os.path.join(dirname, nm)

	#if nm == ipn_util.get_result_dir ():  continue

	fullnm=os.path.join(dirname, nm)
	if (os.path.isdir(fullnm) ): 
		os.system ('rm -rf '+fullnm)
	else: 
		os.unlink (os.path.join(dirname, nm))
# --------------------------------------------------------------------------|

def find_Rerr_file ( key, Rerr_list ):
    for Rerr in Rerr_list:
	if ( re.search ( key, Rerr ) ): return Rerr
    return None
# --------------------------------------------------------------------------|

# check if for each intensity file, ipattern has run successfully.
# Some creiteria:
#   * the corresponding qerr file which should be 0
#   * the corresponding ipn.txt.report file should be generated
#   * the croresponding Rerr file which should be size 79
def check_ipattern_result ( dirname ):
    suffix  = 'ipttn.txt.report'

    reportfiles = ipn_util.myglob (dirname, 'report$')
    intfiles    = ipn_util.myglob (dirname, 'int$')
    qerrfiles   = ipn_util.myglob (dirname, 'qerr$')
    Rerrfiles   = ipn_util.myglob (dirname, 'Rerr$')

    err_list = { } # hold the int file which failed and the reason

    for name in intfiles:
	report_file = name + '.'+suffix
	qerr_file   = name + '.'+'qerr'
	Rerr_file   = find_Rerr_file ( name, Rerrfiles )

	err = [ ]

	if ( not check_report_file(report_file) ): err.append ( report_file )

	if ( len(err) != 0 ): err_list[name] = err

    if len(err_list.keys()) != 0:
	for k in err_list.keys():
	    print >> sys.stderr, \
		     'Intensity file "%s" does not have ipatern result.' % (k)
	print >> sys.stderr, "PLEASE CHECK *.qerr/*.Rerr FILEs UNDER %s "%\
						(os.path.abspath(dirname))
	sys.exit(1)
# -------------------------------------------------------------------------- |

def log_command ( ):
    f = open ( 'ipn_command.log', 'a' )
    print >> f, ' '.join ( sys.argv )
    f.close()
# -------------------------------------------------------------------------- |

def relink (srcname, dstname):
    if ( os.path.exists(dstname) and os.path.islink(dstname) ):
        os.system ( 'unlink ' + dstname)
    else:
	if ( os.path.exists(dstname) and (not os.path.islink(dstname)) ):
	    ipn_util.error ('"'+dstname + '" exists and it is not a link name.')

    os.symlink (srcname, dstname)
# -------------------------------------------------------------------------- |

# source directory contains all the ipattern input files, starting with chr,
# ending with .int, sample.info.txt and sample.stat.txt
# main script starts from here
ipn_util.check_or_exit (ipn_path)

from optparse import OptionParser

arg_parser = OptionParser ( )
arg_parser.add_option ( '-s', '--src-dir', dest='srcdir', action='store',\
		        help='source directory containing intensity files' )
arg_parser.add_option ( '-t', '--temp-dir', dest='tmpdir', action='store',\
		        help='temporary directory contains all the results' )
arg_parser.add_option ( '-o', '--outputfile-name', dest='finalout',
			help='final output file contains all the calls' )
arg_parser.add_option ( '-i', '--sample-information', dest='sampleinfo',
			help='sample information file describing which samples and which samples are in one family' )
arg_parser.add_option ( '--noqsub',
			default=False, action='store_true',
			help='specify if we do not want to run qsub jobs' )

options, args = arg_parser.parse_args()

if ( (not options.srcdir) ):
    arg_parser.print_help()
    sys.exit(1)

noqsub=options.noqsub

# get source directory which contains all the intensity files
srcdir = options.srcdir
srcdir = os.path.abspath ( srcdir )

# check the availability of the source data
if ( not os.path.exists (srcdir) ):
    print >> sys.stderr, 'Source data directory: "'+srcdir+\
			'" does not exist.\n'
    sys.exit(1)


tmpdir = None

if ( options.tmpdir ): tmpdir = options.tmpdir
else:
	print >> sys.stderr, '-t must be set'
	sys.exit(1)

tmpdir = os.path.abspath ( tmpdir ) 

if ( not os.path.exists (tmpdir) ): os.mkdir ( tmpdir )

intfiles = [ ]

# set up links of data files
files = os.listdir ( srcdir )
for nm in files:
    # link data file, but some data needs excluding
    if ( re.search ('\.int$', nm) ):
	intfiles.append ( nm )
	srcname = os.path.join (srcdir, nm) 
	dstname = os.path.join (tmpdir, nm)

	relink (srcname, dstname)
	#copy (srcname, dstname)


    if ( re.search ( 'sample.stats.txt', nm ) ):
	relink ( os.path.join (srcdir, nm), os.path.join (tmpdir, nm) )
	#copy ( os.path.join (srcdir, nm), os.path.join (tmpdir, nm) )

    if ( re.search ( 'matched_names.txt', nm) ):
	relink ( os.path.join (srcdir, nm), os.path.join (tmpdir, nm) )
	#copy ( os.path.join (srcdir, nm), os.path.join (tmpdir, nm) )
# ---

# copy ipattern configuration file
src_conf_file = os.path.join (ipn_path, "ipattern.conf.EXAMPLE")
dst_conf_file = os.path.join (tmpdir, "ipattern.conf")
os.system ('cp ' + src_conf_file + ' ' + dst_conf_file)

#relink ( os.path.join(ipn_path, 'known.cnvr.txt'), 
#		    		os.path.join(tmpdir, 'known.cnvr.txt') )
if os.path.exists(os.path.join(tmpdir, 'known.cnvr.txt')):
 os.remove(os.path.join(tmpdir, 'known.cnvr.txt'))
copy ( os.path.join(ipn_path, 'known.cnvr.txt'), 
		    		os.path.join(tmpdir, 'known.cnvr.txt') )

# setup links of the scripts, as we will run the scripts from 
# that particular directory
#relink ( os.path.join (ipn_path, parser),
#				os.path.join (tmpdir, parser) )
copy ( os.path.join (ipn_path, parser),
				os.path.join (tmpdir, parser) )

#relink ( os.path.join (ipn_path, ip_func),\
#				os.path.join (tmpdir, ip_func) )
copy ( os.path.join (ipn_path, ip_func),\
				os.path.join (tmpdir, ip_func) )

#relink ( os.path.join (ipn_path, ip_run),\
#				os.path.join (tmpdir, ip_run) )

copy ( os.path.join (ipn_path, ip_run),\
				os.path.join (tmpdir, ip_run) )
# special file needed by iPattern to find peak of the algorithm
# this special file ipattern needs to select peak
if ( options.sampleinfo):
    #relink ( os.path.abspath (options.sampleinfo), os.path.join (tmpdir, 'sample.info.txt') )
    copy ( os.path.abspath (options.sampleinfo), os.path.join (tmpdir, 'sample.info.txt') )

#log_command ( )

# switch to the data directory and run!
os.chdir ( tmpdir )

print >> sys.stderr, "Switch directory to:", os.getcwd ()

run_jobs (tmpdir)

print os.getcwd()

check_ipattern_result (tmpdir)

result_dir=ipn_util.get_result_dir()

# everything is Ok, merge result
if ( not os.path.exists ( result_dir) ): os.mkdir ( result_dir )

# move files and merge them
#key=os.path.basename(srcdir) 
#callfile=os.path.join ( result_dir, key+'_all_calls.txt' )
#key=os.getenv ('EXPERIMENT_NAME')
key=os.path.basename(srcdir.rstrip('/')) 
key=re.sub ('_chr', '', key)
callfile= ipn_util.make_ipn_call_file(result_dir, key)

ret = os.system ( "mv chr*.report " + result_dir )
if ( ret != 0 ):
	print >> sys.stderr, 'Running Ipattern, canNOT move report files.'
	sys.exit(1)

merge_cmd = [ 'python', merge_calls, "-d", result_dir, "-o", callfile ]

#
if os.path.exists ( 'matched_names.txt' ):
    merge_cmd += [ '-m', 'matched_names.txt' ]

print >> sys.stderr, ' '.join (merge_cmd)

ret=os.system ( ' '.join ( merge_cmd ) )

if ret != 0:
	print >> sys.stderr, 'Running Ipattern, merging calls FAILed.'
	sys.exit(1)

#
if options.finalout:
    from shutil import copy

    copy (callfile, options.finalout)

#cleanup ( tmpdir )
