import logging
import mido 
import time
import atexit

# see also N:"macOS + MIDI notes"

log = logging.getLogger('midi')


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
        self.on(note = note, velocity = velocity)
        time.sleep(sec_f)
        self.off(note = note)

    def panic(self):    # note that mido has a native method: <port>.panic()
        to_kill = self.active.copy()
        for note in to_kill:
            self.off(note = note)
            time.sleep(0.05)
            log.debug(f"killed note {str(note)}")

class NoteManager():
    pass

