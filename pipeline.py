import multiprocessing
import textools as tt
import pandas as pd
import argparse
import time

from joblib import Parallel, delayed

from use_cases.emotions import process_emotions
from use_cases.contributions import process_contributions
from use_cases.individuals import process_individuals
from use_cases.needs import process_needs

def clean_text(frame):
    # Init Pipeline
    num_cores = multiprocessing.cpu_count()
    print('[INFO] file: {} - {} CPU'.format(opt.data, num_cores))

    # Emotions
    if opt.question == 'P1':
        process_emotions(frame)

    # Contributions
    if opt.question == 'P5':
        process_contributions(frame)

    # Individuals
    if opt.question == 'I':
        process_individuals()

    # Needs dialogues
    if opt.question == 'P4':
        process_needs(frame)

def join_frames(frame_1, frame_2, which=[]):
    # COMBINE FRAMES BY 'ID_USER' COLUMN
    frame_3 = tt.combine_versions(frame_1, frame_2, on='id_user', which=which)

    # SAVE WHOLE FRAME
    frame_3.to_csv('./nuevos_datos/{}.csv'.format(opt.output), index=False) 

    # SAVE A FRAME SAMPLE FOR QUICK READING 
    frame_4  = frame_3.sample(n=1000)
    frame_4.to_csv('./nuevos_datos/{}_tiny.csv'.format(opt.output), index=False)
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', type=str, default='c',
                        help='options: (c)leaning - (j)oin')
    parser.add_argument('--question', type=str, default='P1',
                        help='Question to be proccesed (P1 - P5 - (I)ndividual')
    parser.add_argument('--data', type=str, default='./datos/Dialogo/BBDD_dialogos_final.csv',
                        help='raw data directory')              
    parser.add_argument('--output', type=str, default='emo_per_user',
                        help='output file name')

    # ONLY FOR JOIN MODE
    parser.add_argument('--data2', type=str, default='',
                    help='additional data to be joined with --data')

    # DEBUG MODE IF YOU NEED LOCAL SERVE
    parser.add_argument('--debug', action='store_true', 
                        help='debugger mode')

    opt = parser.parse_args()
    
    # ==========================================
    # ============== READING DATA ==============
    # ==========================================
    t0 = time.time()
    if opt.debug:
        # debug option - smaller dataframe
        chunksize = 100
        frame1 = pd.read_csv(opt.data, chunksize=chunksize, low_memory=False)
        for frame_1 in frame1: break
        if opt.data2 != '':
            frame2 = pd.read_csv(opt.data2, chunksize=chunksize, low_memory=False)
            for frame_2 in frame2: break
    else:
        # all data
        frame_1 = pd.read_csv(opt.data, low_memory=False)
        if opt.data2 != '':
            frame_2 = pd.read_csv(opt.data2, low_memory=False)

    # ==========================================
    # ============== CLEANING MODE =============
    # ==========================================
    if opt.mode == 'c':
        clean_text(frame_1)

    # ==========================================
    # ================ JOIN MODE ===============
    # ==========================================
    if opt.mode == 'j':
        which = ['req_text', 'req_token']
        join_frames(frame_1, frame_2, which)

    t1 = time.time()
    total = t1-t0
    print('[INFO] elapsed time: {:.2f} sec'.format(total))