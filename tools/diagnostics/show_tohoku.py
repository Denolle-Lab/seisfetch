import matplotlib.pyplot as plt

from seisfetch import SeisfetchClient

client = SeisfetchClient(backend="s3_open")
print("Fetching Tohoku eq at PASC (SCEDC)")
start = "2011-03-11T05:30:00"
end = "2011-03-11T12:00:00"
try:
    bundle = client.get_numpy(
        "CI", "RPV", location="00", channel="BHZ", starttime=start, endtime=end
    )
    arrays = bundle.to_dict()
    for tid, data in arrays.items():
        print(f"Trace: {tid}")
        print(f"Max: {data.max()}, Min: {data.min()}")
        plt.figure(figsize=(10, 4))
        plt.plot(data, color="blue", lw=0.5)
        plt.title("Tohoku M9.1 Earthquake recorded at RPV (Riverside, CA)")
        plt.savefig("tohoku_rpv.png")
        print("Saved to tohoku_rpv.png")
        break
except Exception as e:
    print("Failed:", e)
