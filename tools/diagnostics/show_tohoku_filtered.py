import matplotlib.pyplot as plt
import numpy as np
import scipy.signal as signal

from seisfetch import SeisfetchClient

client = SeisfetchClient(backend="s3_open")
print("Fetching Tohoku eq at RPV (SCEDC)")
start = "2011-03-11T05:30:00"
end = "2011-03-11T06:00:00"

try:
    bundle = client.get_numpy(
        "CI", "RPV", location="*", channel="BHZ", starttime=start, endtime=end
    )
    arrays = bundle.to_dict()
    for tid, data in arrays.items():
        print(f"Trace: {tid}")
        sr = 40.0  # standard broad-band sample rate

        # Detrend and remove mean
        data_float = data.astype(np.float64)
        data_float -= np.mean(data_float)
        data_float = signal.detrend(data_float)

        # Filter between 0.05 Hz (20 seconds) and 1.0 Hz
        print("Filtering data between 0.05 Hz and 1.0 Hz...")
        sos = signal.butter(4, [0.05, 1.0], btype="bandpass", fs=sr, output="sos")
        data_filtered = signal.sosfiltfilt(sos, data_float)

        times = np.linspace(0, len(data_filtered) / (sr * 3600), len(data_filtered))

        plt.figure(figsize=(10, 4))
        plt.plot(times, data_filtered, color="black", lw=0.5)
        plt.title(f"Tohoku M9.1 Earthquake (20s - 1Hz) at RPV - {tid}")
        plt.xlabel("Hours since 2011-03-11 05:30:00 UTC")
        plt.ylabel("Amplitude (Filtered)")
        plt.grid(True)
        plt.tight_layout()
        plt.savefig("tohoku_rpv_filtered.png", dpi=200)
        print("Saved to tohoku_pasc_filtered.png")
        break
except Exception:
    import traceback

    traceback.print_exc()
