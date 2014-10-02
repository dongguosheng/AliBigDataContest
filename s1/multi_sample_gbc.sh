#!/bin/bash

# ./sample.py grades_3_larger_4.txt 1
for i in {0..19}; 
    do echo "./gbc.py $i.txt"; ./gbc.py ./sample/grades_3_sample_ratio_1.0_$i.txt grades_3_last.txt;
    mv temp ./gbc_results/$i
done;
./traverse_dir.py ./gbc_results/ out.txt 3350
