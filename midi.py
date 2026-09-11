import logging
import mido 
import time
import atexit

# see also N:"macOS + MIDI notes"

log = logging.getLogger('midi')


msg_breather_s = 0.05       # throttle messages sent at the "same" time


class DeviceNotActive(Exception): pass

class Device():
    """ Known device names: 'Tammy Squared Bluetooth', 'iPad Bluetooth', 'OP-1 Midi Device' """

    def __init__(self, name):
        self.port   = None
        self.active = []        # running set of ON notes

        self._connect(name)
        atexit.register(self.disconnect)     # guarantee connection is closed (on session exit)

    def _connect(self, name):
        devices = mido.get_output_names()

        if name in devices:
            self.port = mido.open_output(name)
        else:
            raise DeviceNotActive

    def disconnect(self):
        self.port.close()   # (noop if already closed)

    def on(self, note, velocity = 64):    # 0-127; Animoog is weird: C2 = 60
        if note not in self.active:
            self.active.append(note)
        self.port.send(mido.Message('note_on', note = note, velocity = velocity))

    def off(self, note):
        self.port.send(mido.Message('note_off', note = note))
        if note in self.active:
            self.active.remove(note)
        else:
            log.debug(f"note {str(note)} not active")

    def tap(self, note, sec_f, velocity = 64):
        """ useful for testing and playing – only supports one note sounding at a time """
        self.on(note = note, velocity = velocity)
        time.sleep(sec_f)
        self.off(note = note)

    def panic(self):    # note that mido has a native method: <port>.panic()
        to_kill = self.active.copy()
        for note in to_kill:
            self.off(note = note)
            time.sleep(msg_breather_s)
            log.debug(f"killed note {str(note)}")

class NoteManager():
    """
    a very simple abstraction for keeping track of how long notes should
    be sounding. NoteManagers do not have their own clock – they keep
    track of what events should happen when, and wait for a parent
    process to ask them when it is time to worry about what should be
    happening (and what NOTE OFFs need to be sent accordingly).

    In the current setup, NoteManagers are device-specific.
    """
    def __init__(self, name, device):
        self.name   = name
        self.device = device
        self.score  = {}

    def sound_note(self, note, velocity, duration_ms):
        time_end = time.time() + (duration_ms / 1000)
        
        # only update the OFF time if it should be _extended_. This is a design
        # choice, but conceivably there will be uses for which we'd want to
        # shorten a note's life instead (will worry about that later).
        if self.score.get(note) is None or time_end > self.score[note]:
            self.score[note] = time_end
        
        self.device.on(note, velocity)

    def elapse_events(self, time_now = None):
        """ 
        figure out which events should have happened by `time_now`, trigger
        them, and clean up the `score` accordingly. If no specific time
        is supplied, use the current time.
        """
        if time_now is None:
            time_now = time.time()

        elapsed_events = [ note for note, expiry in self.score.items() if expiry <= time_now ]

        for note in elapsed_events:
            self.device.off(note)
            del self.score[note]
            time.sleep(msg_breather_s)

    def wipe_off(self):
        """ send NOTE OFFs for any still-sounding notes, regardless of the time """
        log.debug(f"NoteManager {self.name} wipe_off ({len(self.score)} notes)")

        for note in self.score.keys():
            self.device.off(note)
            time.sleep(msg_breather_s)

        self.score = {}
