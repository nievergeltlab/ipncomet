#Extract Ipattern
 tar xvzf pbs_ipn_0.581.tar.gz

#Setup 1) You have to make some changes to the iPattern scripts in order for it to be compatible with Comet:

#lines 37-40 of ipn/run_ipattern.py: Where there are '$', replace with 'PBS'
 #Reason: We explicitly want to say it's the PBS scheduling system. This will allow comet to conver the parameters to SLURM paramters
 
#Line 40 of ipn/run_ipattern.py: It says:   print >> outf, "#PBS -l h_vmem=8G"    . Replace it with this:  print >> outf, "#PBS -l vmem=8G"
 #Reason: vmem command is compatible with comet, but h_vmem is not
 
#Line 41 of ipn/run_ipattern.py: Add     print >> outf, "#PBS -l walltime=6:00:00" 
 #Reason: Need to set a walltime in order for iPattern to run. 6 hours seems like enough given my test runs. May need be to changed if this runs out of time
 
#Line 275 of ipn/run_ipattern.py: One the lines before it says 'copy ( os.path.join(ipn_path, 'known.cnvr.txt')...', add in these two lines of code:
 
#if os.path.exists(os.path.join(tmpdir, 'known.cnvr.txt')):
# os.remove(os.path.join(tmpdir, 'known.cnvr.txt'))

 #Reason: For some reason when this file link gets created, it only has permission to read. If it tries to create the link again, it can't overwrite. and will crash. This deletes the link if it exists already, so it can create it again

#Line 22 of ipnlib/ipn_pbs_qsub.py: Change sleep time from 30 to 120
#Line 452 of ipnlib/ipn_pbs_qsub.py: Change sleep time from 60 to 120
#Line 714 of ipnlib/ipn_pbs_qsub.py: Change sleep time from 60 to 120
 #Reason: It polls the job status system too quickly, making it appear as if some jobs hadn't been submitted, which causes iPattern to fail.
 
#line 262 of ipnlib/ipn_pbs_qsub.py: replace 24 hours with 2 hours, e.g. print >> f, '#PBS -l walltime=2:00:00' 
 #Reason: set walltime down from 24 hours to 2. 24 is absurdly long and will delay you in the queue. If you run out of time, just up this value.

 #Line 713-714 of ipnlib/ipn_pbs_qsub.py:	says "
 #if not has_jobs_running (waiting_files.keys(), current_jobs) and \
	#					cur >= len(job_files): break
 
 #Line 61 of ipn/iPattern.Runner.R , comment.char = ""
 #Reason: If you have # characters in SNP names, which happens sometimes, it'll crash. This makes it so the hashes are read as characters and not comments.


#Setup2) Open .bashrc file in your home directory. Export iPattern paths in .bashrc file, e.g. 

# export IPNBASE="/home/amaihofer/ipn_0.581"
# export PATH=$PATH:"$IPNBASE/ipn"
# export PATH=$PATH:"$IPNBASE/preprocess/ilmn"
# export PATH=$PATH:"$IPNBASE/preprocess/affy"
# export PYTHONPATH=$PYTHONPATH:"$IPNBASE/ipnlib"

#But without the hash tags included. Replace amaihofer with your username.


#Setup 3) Use shortcuts to tell iPattern that you want to use pbs job scripts , and NOT SGE scripts.

 cd "$IPNBASE"/ipnlib 
 ln -s ipn_pbs_qsub.py ipn_qsub.py #you may have to delete existing ipn_qsub.py link for this to work

#Setup 4) Install ppc R library (only has to be done one time)

 module load R
 wget http://statweb.stanford.edu/~tibs/PPC/Rdist/ppc_1.02.tar.gz
 R
 install.packages('ppc_1.02.tar.gz',repos=NULL)


#iPattern test run code example:

module load R #important to load R every time prior to running ipattern, otherwise it won't find R and fail.
module load python #important to load python (ipattern uses numpy) prior to running ipattern, otherwise it will fail because it can't load numpy.

#Note: For running the job, the scratch space should be /oasis/scratch/comet/$USER/ , as that is the designated scratch space

#There are two ways of running iPattern:
#For whichever way you run it, a bunch of shortcut files will be made in whatever directory you launch from, so I like to start within a specific folder, e.g.
study=gtpc
 mkdir $study
 cd  $study
 
#Put intensity files in the temp folder 
#Use the script to make intensity file lists..
mkdir -p /oasis/scratch/comet/$USER/temp_project/"$study"/intensities

Rscript make_intensitylists_v1.r ressler_gtp_cnv.csv /oasis/scratch/comet/$USER/temp_project/"$study"/intensities $study


#Method 1: From the shell, as a parameterized command.
#Here I set variables to paths
#Note: Do not have carriage returns (windows style returns) in this file, the bad sample file, or data list file. run dos2unix on them first if they came from excel
 gender_file=/home/amaihofer/"$study"/gender_file.txt #Path to gender file.
 bad_sample_file=/home/amaihofer/"$study"/bad_samples.txt #Path to bad sample file
 data_file_list=/home/amaihofer/"$study"/"$study"_intensities #List of intesity data files. Should be named for doing a loop!
 split=no #Split data? set to no if this is not needed
 probe_file=xxxx #Path to probe file. Set to xxxx if unused
 batch_file=xxxx #path to batch file. Set to xxxx if unused
 output_dir=/home/amaihofer/"$study"/results #Path to output results 
 experiment="$study"  #experiment signature
 tempdir=/oasis/scratch/comet/$USER/temp_project

 if [ $split != "no" ]
 then
  split_command="--split"
 fi

 if [ $probe_file != "xxxx" ]
 then
  probe_command="--p $probe_file"
 fi

 if [ $batch_file != "xxxx" ]
 then
  batch_command="--b $batch_file"
 fi

 for i in $(ls *intensities* | wc -l | sort) # for each intensity file set, run this command..
 do
  ilmn_run.py -g $gender_file -m $bad_sample_file -f "$data_file_list"_"$i" -x $experiment"_$i" $batch_command $split_command $probe_command --temp-prefix-directory-name $tempdir --dest-prefix-directory-name $tempdir --call-prefix-directory-name $tempdir  --out "$output_dir"_"$i"
 done
#Method 2) To use configuration files instead of supplying paramters to the command line
ilmn.sh /home/amaihofer/test.conf
