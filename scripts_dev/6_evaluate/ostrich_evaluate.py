import sys, subprocess, shutil, re, shutil
from pathlib import Path
import os

# Parser import (liegt bei dir unter scripts/parse_eval_output.py)
sys.path.append("/starvers_eval/scripts")
from ostrich_parse_eval_output import parse_eval_stdout_to_csv  # <-- Name angepasst

PAT_ADDED   = re.compile(r"data-added_(\d+)-(\d+)\.nt$")
PAT_DELETED = re.compile(r"data-deleted_(\d+)-(\d+)\.nt$")

IMAGE = "ostrich:v1"

DEFAULT_REPS = 3
DATASETS = sys.argv[1].split(" ")

def sh(cmd, capture=False, **kwargs):
    print("+", " ".join(map(str, cmd)), flush=True)
    if capture:
        result = subprocess.run(
            cmd,
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            **kwargs
        )
        return result.stdout.strip()
    else:
        subprocess.run(cmd, check=True, **kwargs)
        return None
    
def ensure(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)
    return p

def evaluation(dataset):
    # Host-Pfade (im evaluate-Container als Bind-Mounts vorhanden)
    rawdata_root      = Path("/starvers_eval/rawdata").resolve()
    queries_dir_host  = Path("/starvers_eval/ostrich/queries_bearb").resolve()
    temp_root         = Path("/starvers_eval/ostrich_temp").resolve()
    results_root_host = Path("/starvers_eval/ostrich_results").resolve()

    # Eingaben
    dataset_root = rawdata_root / dataset
    dataset_dir  = dataset_root / "alldata.CB_computed.nt"  # enthält Patch-Verzeichnisse

    if not dataset_dir.exists():
        sys.exit(f"Dataset dir not found: {dataset_dir}")

    # Temp-Ziel vorbereiten: /starvers_eval/ostrich_temp/<dataset>
    temp_dataset_dir = temp_root / dataset
    if temp_dataset_dir.exists():
        shutil.rmtree(temp_dataset_dir)
    shutil.copytree(dataset_dir, temp_dataset_dir)

    # Snapshot-Datei in snapshot.nt kopieren
    ic_dir = dataset_root / "alldata.IC.nt"
    # robust: 000001.nt ODER 00001.nt
    candidates = [ic_dir / "000001.nt", ic_dir / "00001.nt", ic_dir / "1.nt"]
    src_snapshot = next((p for p in candidates if p.exists()), None)
    if src_snapshot is None:
        sys.exit(f"No snapshot file found in {ic_dir} (tried {', '.join(map(str, candidates))})")

    dst_snapshot = temp_dataset_dir / "snapshot.nt"
    shutil.copy(src_snapshot, dst_snapshot)

    dst = temp_dataset_dir / "computed_patches"
    if dst.exists():
        shutil.rmtree(dst)
    dst.mkdir(parents=True)


    # Snapshot -> patch 0
    snap = temp_dataset_dir / "snapshot.nt"
    if not snap.exists():
        sys.exit("ERROR: snapshot.nt not found")
    dst_file = ensure(dst/"0"/"main.nt.additions.txt")
    with open(snap, "r", encoding="utf-8") as fin, open(dst_file, "w", encoding="utf-8") as fout:
        for line in fin:
            if line.lstrip().startswith("#"):
                continue  # Kommentar überspringen
            if not line.strip():
                continue  # leere Zeilen überspringen
            fout.write(line)

    (dst/"0"/"main.nt.deletions.txt").write_text("", encoding="utf-8")
    
    (ensure(dst/"1"/"main.nt.additions.txt")).write_text("", encoding="utf-8")
    (dst/"1"/"main.nt.deletions.txt").write_text("", encoding="utf-8")

    # Collect patches
    added, deleted = {}, {}
    for f in temp_dataset_dir.iterdir():
        mA = PAT_ADDED.match(f.name)
        mD = PAT_DELETED.match(f.name)
        if mA:
            _, y = mA.groups()
            added[int(y)] = f
        elif mD:
            _, y = mD.groups()
            deleted[int(y)] = f

    for pid in sorted(set(added)|set(deleted)):
        d = dst/str(pid)
        if pid in added:
            shutil.copyfile(added[pid], ensure(d/"main.nt.additions.txt"))
        else:
            ensure(d/"main.nt.additions.txt").write_text("")
        if pid in deleted:
            shutil.copyfile(deleted[pid], ensure(d/"main.nt.deletions.txt"))
        else:
            ensure(d/"main.nt.deletions.txt").write_text("")

    print(f"Patches prepared under {dst}")



    # Patch-Range bestimmen
    patch_dirs = [int(p.name) for p in dst.iterdir() if p.is_dir() and p.name.isdigit()]
    if not patch_dirs:
        sys.exit(f"No patch folders found in {dst}")
    start_idx, end_idx = 0, max(patch_dirs)

    # Container-Mounts (so wie wir run -v setzen)
    #  -v /starvers_eval/ostrich_temp:/data
    #  -v /starvers_eval/ostrich/queries_bearb:/queries
    #  -v /starvers_eval/ostrich_results:/results
    data_mount_host   = dst           # Host-Seite
    data_mount_in     = "/data"             # Container-Seite
    dataset_in_cont   = f"{data_mount_in}/{dataset}"  # /data/<dataset>
    queries_in_cont   = "/starvers_eval/ostrich/queries_bearb"
    results_in_cont   = "/starvers_eval/ostrich_results"

    results_dir = results_root_host / dataset
    results_dir.mkdir(parents=True, exist_ok=True)

    out = sh(["du", "-sh", dst], capture=True)
    size_only = out.split()[0]
    with open(f"/starvers_eval/ostrich_results/{dataset}/file_size.txt", "w") as f:
        f.write(size_only + "\n")

    store = Path("/store").resolve()
    store.mkdir(parents=True, exist_ok=True)

    # Alle Query-Dateien nacheinander evaluieren
    for qfile in sorted(queries_dir_host.glob("*.txt")):
        log_path = results_dir / f"{qfile.stem}.log"

        cmd = [
            "bash", "-lc",
            f"cd {store} && /opt/ostrich/build/ostrich-evaluate {dst} {start_idx} {end_idx} {queries_in_cont}/{qfile.name} {DEFAULT_REPS} | tee {log_path}"
            f""
        ]
        sh(cmd, capture=False)

        # Logs -> CSVs
        parse_eval_stdout_to_csv(
            log_path,
            results_dir / f"{qfile.stem}_insertion.csv",
            results_dir / f"{qfile.stem}_vm.csv",
            results_dir / f"{qfile.stem}_dm.csv",
            results_dir / f"{qfile.stem}_vq.csv",
        )

if __name__ == "__main__":
    for dataset in DATASETS:
        evaluation(dataset)
