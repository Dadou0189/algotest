from time import sleep, time

from mpi4py import MPI

from enums import MessageTag, REPLCmdCode, REPLSpeedModeCode, REPLSpeedModeValues
from utils import read_commands_from_file, concat_logs
from command import Command

_LOG_BASE_FNAME = "server{sid}_log.txt"
_COMMAND_BASE_FNAME = "client{sid}_commands.txt"
_REPL_UID = 0


def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    servers_amount = (size - 1) // 2
    clients_amount = size - servers_amount - 1
    last_index = [0 for i in range(size)] # last index of the logs for servers and of the commands for clients
    server_logs = [] 
    if rank <= servers_amount:  # servers
        timeout = REPLSpeedModeValues.MEDIUM.value
        crashed = False
        while True:
            status = MPI.Status()
            if crashed:
                if comm.Iprobe(source=_REPL_UID, tag=MessageTag.REPL, status=status):
                    cmd = comm.recv(source=_REPL_UID, tag=MessageTag.REPL)
                    code = int(cmd[0])
                    if code == REPLCmdCode.RECOVERY:
                        crashed = False
                        logs = []
                        for server_uid in range(1, 1 + servers_amount):
                            if server_uid != rank:
                                if comm.Iprobe(source=MPI.ANY_SOURCE, tag=MessageTag.SERVER, status=status):
                                    comm.send("REQUEST_STATE", dest=server_uid, tag=MessageTag.SERVER)
                                    state = comm.recv(source=server_uid, tag=MessageTag.SERVER)
                                    if state != []:
                                        logs.append(state)
                                        server_logs.append(state)
                                        break
                        if logs:
                            merged_log = concat_logs(logs)
                            with open(_LOG_BASE_FNAME.format(sid=rank), "a+") as f:
                                f.writelines(merged_log)
            else:
                if comm.Iprobe(source=_REPL_UID, tag=MessageTag.REPL, status=status):
                    cmd = comm.recv(source=_REPL_UID, tag=MessageTag.REPL)
                    code = int(cmd[0])
                    if code == REPLCmdCode.CRASH:
                        crashed = True
                        server_logs = []
                    elif code == REPLCmdCode.SPEED:
                        mode = REPLSpeedModeCode(int(cmd[1]))
                        timeout = REPLSpeedModeValues[mode.name].value
                elif comm.Iprobe(source=MPI.ANY_SOURCE, tag=MessageTag.CLIENT, status=status):
                    client_uid = status.Get_source()
                    client_cmd = comm.recv(source=client_uid, tag=MessageTag.CLIENT)
                    cmd = Command.verify(client_cmd, client_uid, time())
                    if cmd:
                        server_logs.append(f"{cmd.cmd_to_logs()}\n")
                        with open(_LOG_BASE_FNAME.format(sid=rank), "a+") as file:
                            file.write(f"{cmd.cmd_to_logs()}\n")
                    sleep(timeout)
                elif comm.Iprobe(source=MPI.ANY_SOURCE, tag=MessageTag.SERVER, status=status):  # Send logs in response
                    request = comm.recv(source=MPI.ANY_SOURCE, tag=MessageTag.SERVER)
                    if request == "REQUEST_STATE":
                        comm.send(server_logs, dest=status.Get_source(), tag=MessageTag.SERVER)
    else:  # clients
        crashed = False
        started = False
        while True:
            status = MPI.Status()
            if comm.Iprobe(source=_REPL_UID, tag=MessageTag.REPL, status=status):
                cmd = comm.recv(source=_REPL_UID, tag=MessageTag.REPL)
                code = int(cmd[0])
                if code == REPLCmdCode.START or (code == REPLCmdCode.RECOVERY and crashed and started):
                    started = True
                    try:
                        cmds = read_commands_from_file(_COMMAND_BASE_FNAME.format(sid=rank))
                        if crashed:
                            crashed = False
                            cmds = cmds[last_index[rank]:]
                        for cmd in cmds:
                            if crashed: # if the client has crashed in the meantime stop sending commands
                                break 
                            for server_uid in range(1, 1 + servers_amount):
                                comm.send(cmd, dest=server_uid, tag=MessageTag.CLIENT)
                            last_index[rank] += 1 # increment the number of command sended    
                    except FileNotFoundError:
                        print(f"Client{rank} doesn't have a commands file")
                elif code == REPLCmdCode.CRASH: # for now, a crashed serves means it is down
                    crashed = True
            else:  # unhandled repl message
                pass

                
if __name__ == "__main__":
    main()