# ostrich_sparql_server.py
import os, subprocess, time, requests
from typing import Optional


class OstrichServer:
    def __init__(self, image: str, store_dir: str,
                 host_port: int = 3000, host: str = "127.0.0.1",
                 container_port: int = 42564, container_name: Optional[str] = None,
                 start_timeout_s: float = 60.0, network: Optional[str] = "ostrich_net"):
        self.image = image
        self.store_dir = os.path.abspath(store_dir)
        self.host_port = int(host_port)
        self.host = host
        self.container_port = int(container_port)
        self.container_name = container_name or f"ostrich-spql-{self.host_port}"
        self.start_timeout_s = start_timeout_s
        self.network = network
        self._running = False

    @property
    def endpoint_url(self) -> str:
        return f"http://{self.container_name}:{self.container_port}/sparql"

    def start(self):
        if not os.path.isdir(self.store_dir):
            raise RuntimeError(f"Store dir not found: {self.store_dir}")

        # build run command
        cmd = ["docker", "run", "-d", "--rm",
               "--name", self.container_name,
               "-v", f"{self.store_dir}:/var/ostrich:ro"]
        if self.network:
            cmd += ["--network", self.network]
        else:
            cmd += ["-p", f"0.0.0.0:{self.host_port}:{self.container_port}"]

        cmd += [self.image]
        print("docker run:", " ".join(cmd))

        run = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        if run.returncode != 0:
            raise RuntimeError(f"Docker run failed: {run.stdout}")
        self._running = True

        # >>> HEALTH WAIT (poll the SAME URL the client will use)
        
        deadline = time.time() + self.start_timeout_s
        url = f"{self.endpoint_url}?query=SELECT%20%2A%20WHERE%20%7B%20GRAPH%20%3Cversion%3A0%3E%20%7B%20%3Fs%20%3Fp%20%3Fo%20.%20%7D%20%7D%20LIMIT%2010"
        while time.time() < deadline:
            ps = subprocess.run(["docker","inspect","-f","{{.State.Running}}", self.container_name],
                                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
            if ps.returncode == 0 and ps.stdout.strip() != "true":
                logs = subprocess.run(["docker","logs","--tail","200", self.container_name],
                                      stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                raise RuntimeError(f"Server container exited early:\n{logs.stdout}")
            try:
                r = requests.get(url, timeout=2.0)
                if r.status_code == 200:
                    print("Endpoint healthy at:", self.endpoint_url)
                    return
            except Exception:
                pass
            time.sleep(0.25)

        logs = subprocess.run(["docker","logs","--tail","200", self.container_name],
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        raise RuntimeError(f"Endpoint not healthy after {self.start_timeout_s}s.\n{logs.stdout}")
        

    def stop(self):
        if not self._running:
            return
        subprocess.run(["docker","stop","-t","5", self.container_name],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self._running = False
