import multiprocessing
import textools as tt
import pandas as pd
import argparse

from joblib import Parallel, delayed

def clean_text(frame):
    # Init Pipeline
    num_cores = multiprocessing.cpu_count()
    print('[INFO] file: {} - {} CPU'.format(opt.data, num_cores))

    if opt.question == 'P1':
        from use_cases.emotions import process_one
    if opt.question == 'P5':
        from use_cases.requirements import process_one

    # RUN SCRIPT FOR EACH SURVEY
    allframes = Parallel(n_jobs=num_cores)(delayed(process_one)(row, k) \
                    for k, row in frame.iterrows())
    # FILTERING NAN VALUES
    allframes = [f for f in allframes if f is not None]
    # CONCATENATE ALL FRAMES
    new_data = pd.concat(allframes, 0)
    # IF EXIST AGES THEN STRATIFY THEM
    if 'age' in new_data.columns:
        new_data = tt.stratify_frame_by_age(new_data)
    # SAVE NEW FRAME
    new_data.to_csv('./nuevos_datos/{}.csv'.format(opt.output), index=False)

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
    parser.add_argument('--data', type=str, default='./datos/Dialogo/BBDD_dialogos_final.csv',
                        help='raw data directory')
    parser.add_argument('--question', type=str, default='P1',
                        help='Question to be proccesed (P1 - P5)')              
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
    if opt.debug:
        # debug option - smaller dataframe
        chunksize = 10
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