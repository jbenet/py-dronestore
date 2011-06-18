
import smhasher

def hash(tohash):
  return smhasher.murmur3_x86_64(str(tohash))