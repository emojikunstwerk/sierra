"""
conda create -n sierra pandas python=3.14
conda activate sierra
python3 -m pip install mido
python3 -m pip install python-rtmidi


conda activate sierra
python -i sierra.py
"""

from midi import Device
from midi import DeviceNotActive
from midi import NoteManager
from midi import mido
import pandas as pd
import numpy as np
import time
import logging
import sys

log   = None    # log instance 
tammy = None    # mido device port will be stored here
data  = None    # processed snotel data will be stored here
rep   = None    # properties characterizing map from `data` to MIDI range, ie, the representation

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
octave_step = 12        # notes per octave
octave_range = [0, 5]   # data map should span X octaves

note_duration_range = [750, 1000 * 15]    # ms
note_velocity_range = [7, 127]

year_range = [2014, 2019]


def setup(debug = True, dev_name = None):
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
    log.info('\n\n\nNew session\n')

    load_data()
    make_map()
    connect_tammy(dev_name)

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

def make_map():
    global rep, data

    palette = make_note_palette()

    data = data.assign(
        water_log = np.log(data['water_mm_sum'] + 0.01)
        # temp_log  = np.log(data['temp_c_med']   + 0.01)
    )
    
    rep = {
        'note_palette': palette,
        'note_i_max':   len(palette) - 1,

        'water_min':    data['water_log'].min(),
        'water_max':    data['water_log'].max(),

        'temp_min':     data['temp_c_med'].min(),
        'temp_max':     data['temp_c_med'].max()
    }

    rep.update({
        'water_delta':      rep['water_max'] - rep['water_min'],
        'temp_delta':       rep['temp_max']  - rep['temp_min'],
        'duration_delta':   note_duration_range[1] - note_duration_range[0],
        'velocity_delta':   note_velocity_range[1] - note_velocity_range[0]
    })

def make_note_palette():
    """
    the scale is arbitrary, and the octave range is constrainted. this
    function pre-generates an ordered set of allowed notes, the indexes
    of which are the targets of a data column map
    """
    palette = []

    for note in notes.values():
        for octave_i in range(octave_range[0], octave_range[1]):
            nt = note + (octave_i * octave_step)
            palette.append(nt)

    palette.sort()

    return palette

def station_to_midi(st):
    """ 
    the logic to map the observation values to synthesizer parameters.
    this is arbitrary, experimental, expressing, and the entire point.
    """
    log.debug(f'\n{st}')


    # the nominal, direct maps
    note_i = int(
        (rep['water_delta'] -                           # invert
            (st.water_log - rep['water_min']) ) /       # offset
        rep['water_delta'] *                            # unit normalize
        rep['note_i_max']                               # expand (map to palette)
    )
    
    cc_val = int(
        (st.temp_c_med - rep['temp_min']) /         # offset
        rep['temp_delta'] *                         # unit normalize
        127                                         # expand (CC range)
    )

    # correlate other sounding properties to the note
    velocity = int(
        (rep['note_i_max'] - note_i) /      # invert
        rep['note_i_max'] *                 # unit normalize
        rep['velocity_delta'] +             # scale
        note_velocity_range[0]              # offset
    )
    dur = int(
        (rep['note_i_max'] - note_i) /      # invert
        rep['note_i_max'] *                 # unit normalize
        rep['duration_delta'] +             # scale
        note_duration_range[0]              # offset
    )


    output_values = {
        'note':     rep['note_palette'][ note_i ], 
        'velocity': velocity, 
        'note_dur': dur,                        # ms

        'cc':       '64',                       # fixed parameter
        'cc_val':   cc_val
    }

    log.debug(f'\n{output_values}\n')

    return output_values

def start(longitude_group, elev_rest = elev_rest):

    score = NoteManager("sierra", tammy)

    voice_data = data.query(f'longitude_group == {longitude_group}')
    log.info(f'Loaded longitude_group {longitude_group} ({len(voice_data)} obs)')

    for year in range(year_range[0], year_range[1] + 1, 1):

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
print('Call setup() [sierra.log debug mode on by default], then start(longitude_group)')
