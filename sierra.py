"""
conda create -n sierra pandas python=3.14
conda activate sierra
python3 -m pip install mido
python3 -m pip install python-rtmidi
"""

from midi import Device
from midi import DeviceNotActive
from midi import NoteManager
import pandas as pd
import time
import logging
import sys

log   = None    # log instance 
tammy = None    # mido device port will be stored here
data  = None    # processed snotel data will be stored here

elev_rest           = 5         # ms, step time between feet of elevation
section_rest        = 4000      # ms, rest time between sections
start_elevation_ft  = 9500      # imaginary elevation at which playhead begins

notes = {   # note @ lowest permitted octave (to simplify metric mapping)
    'C':    36,
    'D':    38,
    'F':    41,
    'G':    43,
    'A':    45 
}
octave_step = 12
octave_range = [0, 5]

note_dur_range = [750, 1000 * 10]    # ms

def setup(debug = True):
    global log

    handler_console = logging.StreamHandler(sys.stdout)
    handler_console.setLevel(logging.INFO)

    handler_file = logging.FileHandler(filename = 'sierra.log', mode = 'a')
 
    logging.basicConfig(
        level   = logging.DEBUG if debug else logging.INFO,
        format  = "%(asctime)s %(levelname)s: %(message)s",
        handlers = [handler_console, handler_file]
    )

    log = logging.getLogger('sierra')

    load_data()
    connect_tammy()

def connect_tammy(dev_name = None):
    global tammy
    
    if dev_name is None:
        dev_name = 'Tammy Squared Bluetooth'
    if tammy is None:
        try:
            tammy = Device(dev_name)
            log.info(f'Tammy connected as {dev_name}.')
        except DeviceNotActive:
            log.warning(f"Tammy not active (as \'{dev_name}\'). Try again: mido.get_output_names() / connect_tammy(\'new name\')")
    else:
        log.info("Tammy already connected.")

def load_data():
    global data

    if data is None:
        data = pd.read_csv("sierra-prep.csv")

def send_midi(note, velocity, note_dur, cc, cc_val):
    # send CC data ...

    pass

def station_to_midi(station_row):
    log.debug(f'\n{station_row}')

    return {
        'note':     station_row.water_mm_sum, 
        'velocity': station_row.water_mm_sum, 
        'note_dur': station_row.water_mm_sum,
        'cc':       '64',                       # fixed for now
        'cc_val':   station_row.temp_c_med
    }


def start(longitude_group, elev_rest = elev_rest):

    score = NoteManager("sierra", tammy)

    voice_data = data.query(f'longitude_group == {longitude_group}')
    log.info(f'Loaded longitude_group {longitude_group} ({len(voice_data)} obs)')

    for year in ['2014', '2015', '2016', '2017', '2018', '2019']:

        section = voice_data.query(f'year == {year}')
        section_n = len(section)
        log.info(f'Loaded year {year} ({section_n} obs) --------------------------------------')

        # the inner playback loop(s) are engineered naively (ie, respective of
        # loop conclusion vs. strict global metronome, and inefficiently WRT
        # typical pythonic loops, but this is to solve for a) _compositional_
        # requirements, and b) simplicity (not strict time correctness; small
        # wiggle is ok)

        next_station_i = 0

        for elev_playhead in range(start_elevation_ft, 0, -1):
            # log.debug(f'elev_playhead: {elev_playhead}')

            if (next_station_i <= (section_n - 1) and 
                section.iloc[next_station_i].elevation_ft >= elev_playhead):
                
                ev = station_to_midi( section.iloc[next_station_i] )
                # do CC stuff
                score.sound_note(ev['note'], ev['velocity'], ev['note_dur'])

                next_station_i = next_station_i + 1

            time.sleep(elev_rest / 1000)
            score.elapse_events(time.time())

        score.wipe_off()    # just in case
        log.info('Resting between years...')
        time.sleep(section_rest / 1000)

    log.info(f'Completed run (longitude_group {longitude_group})')

# setup()
print('Call setup() to get started (sierra.log debug mode on by default)')
