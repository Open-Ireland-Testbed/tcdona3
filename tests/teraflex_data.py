"""CLI outputs of a TeraFlex, shaped like the device's ``show ... pm current``; the values are made up."""

PROMPT = "admin@FSP3000C> "

PM_OPT_PHY = """show interface 1/1/n1 opt-phy pm current

pm current table:

mon-entity  interval  pm-profile  max-intervals  tca-transient  suspect      elapsed
opt-phy     live      IFTFnw      0              false          not-suspect  1:02:03:04 d:h:m:s
  pm-name     loc   curr-value  thr-low    -high    -range      -def
  opt-rx-pwr  nend  -12.5 dBm                                   N/A
  opt-tx-pwr  nend  0.5 dBm     -13.0 dBm  7.0 dBm  -13.0..7.0  global

mon-entity  interval  pm-profile  max-intervals  tca-transient  suspect      elapsed
opt-phy     15min     Power       96             true           suspect      0:00:01:44 d:h:m:s
  pm-name          loc   curr-value  thr-low  -high  -range  -def
  opt-rx-pwr-mean  nend  -12.9 dBm                           N/A
  opt-tx-pwr-mean  nend  0.4 dBm                             N/A

"""


def pm_otsi(snr="18.20", osnr="30.10", q="9.40", osnr_first=False, stale_first=False):
    snr_row = "  signal-to-noise-ratio          nend  %s dB     0.0 dB    0.0 dB   0.0..50.0  global\n" % snr
    osnr_row = "  optical-signal-to-noise-ratio  nend  %s dB     0.0 dB    0.0 dB   0.0..60.0  global\n" % osnr
    rows = osnr_row + snr_row if osnr_first else snr_row + osnr_row
    stale = """
mon-entity  interval  pm-profile  max-intervals  tca-transient  suspect      elapsed
1           15min     QualityTF   96             true           not-suspect  0:00:05:00 d:h:m:s
  pm-name                        loc   curr-value  thr-low   -high    -range     -def
  q-factor                       nend  1.00 dB     4.0 dB    0.0 dB   4.0..20.0  global
"""
    return ("pm current table:\n" + (stale if stale_first else "") + """
mon-entity  interval  pm-profile  max-intervals  tca-transient  suspect      elapsed
1           live      QualityTF   0              false          not-suspect  3:16:32:24 d:h:m:s
  pm-name                        loc   curr-value  thr-low   -high    -range     -def
  carrier-frequency-offset       nend  N/A         -2.5 GHz  2.5 GHz  -3.2..3.2  global
""" + rows + "  q-factor                       nend  %s dB     4.0 dB    0.0 dB   4.0..20.0  global\n" % q + ("" if stale_first else stale) + "\n")


def pm_otuc(layer="otuc4-p"):
    return """pm current table:

mon-entity  interval  pm-profile  max-intervals  tca-transient  suspect      elapsed
%s     live      FECotu      0              false          not-suspect  0:01:00:00 d:h:m:s
  pm-name   loc   curr-value  thr-low  -high  -range  -def
  fec-ber   nend  1.2E-04                               N/A

mon-entity  interval  pm-profile  max-intervals  tca-transient  suspect      elapsed
%s     15min     FECotu15    96             true           not-suspect  0:00:05:00 d:h:m:s
  pm-name        loc   curr-value  thr-low  -high  -range  -def
  fec-ber-mean   nend  1.5E-04                               N/A

""" % (layer, layer)


def device(outputs):
    """A function ``command -> output`` for :class:`fakes.FakeShell`: the output with CRLF line ends and the
    prompt, picked by the first key of ``outputs`` found in the command (normalised spaces)."""
    def answer(command):
        command = " ".join(command.split())
        for key, output in outputs.items():
            if key in command:
                return output.replace("\n", "\r\n") + PROMPT
        return PROMPT
    return answer
