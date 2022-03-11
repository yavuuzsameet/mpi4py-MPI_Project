#Yavuz Samet Topcuoglu
#compiling: yes
#working: yes

#This project is done in Lubuntu virtual machine.
#The MPI is installed using pip from terminal.
#The installed python version is 2.7.17

#run the program as follows:
#mpiexec -n [P] python main.py [input] [output]
#where P is number of processors, input is path of the input file, and output is the path of the output file.

from mpi4py import MPI
import sys

#left function checks the left cell of the current cell.
#returns the damage taken.
def left(i,j,type_data,tower):
    if j==0: return 0
    else:
        if tower == '+' and type_data[i][j-1] == 'o': return 1
        if tower == 'o' and type_data[i][j-1] == '+': return 2
        else: return 0

#right function checks the right cell of the current cell.
#returns the damage taken.
def right(i,j,N,type_data,tower):
    if j==N-1: return 0
    else:
        if tower == '+' and type_data[i][j+1] == 'o': return 1
        if tower == 'o' and type_data[i][j+1] == '+': return 2
        else: return 0

#up function checks the above cell of the current cell.
#returns the damage taken.
def up(i,j,type_data, tower, above_type):
    if i==0:
        if rank==1: return 0
        else:
            if tower == '+' and above_type[j] == 'o': return 1
            if tower == 'o' and above_type[j] == '+': return 2
            else: return 0

    else:
        if tower == '+' and type_data[i-1][j] == 'o': return 1
        if tower == 'o' and type_data[i-1][j] == '+': return 2
        else: return 0

#down function checks the below cell of the current cell.
#returns the damage taken.
def down(i,j,thickness,type_data, tower, below_type):
    if i==thickness-1:
        if rank==worker: return 0
        else:
            if tower == '+' and below_type[j] == 'o': return 1
            if tower == 'o' and below_type[j] == '+': return 2
            else: return 0

    else:
        if tower == '+' and type_data[i+1][j] == 'o': return 1
        if tower == "o" and type_data[i+1][j] == '+': return 2
        else: return 0

#topleft function checks the upper-left cell of the current cell.
#returns the damage taken.
def topleft(i,j,type_data,above_type):
    if j==0: return 0
    if i==0:
        if rank==1: return 0
        else:
            if above_type[j-1] == 'o': return 1
            else: return 0

    else:
        if type_data[i-1][j-1] == 'o': return 1
        else: return 0

#topright function checks the upper-right cell of the current cell.
#returns the damage taken.
def topright(i,j,N,type_data,above_type):
    if j==N-1: return 0
    if i==0:
        if rank==1: return 0
        else:
            if above_type[j+1] == 'o': return 1
            else: return 0

    else:
        if type_data[i-1][j+1] == 'o': return 1
        else: return 0

#bottomleft function checks the bottom-left cell of the current cell.
#returns the damage taken.
def bottomleft(i,j,thickness,type_data,below_type):
    if j==0: return 0
    if i==thickness-1:
        if rank==worker: return 0
        else:
            if below_type[j-1] == 'o': return 1
            else: return 0

    else:
        if type_data[i+1][j-1] == 'o': return 1
        else: return 0

#bottomright function checks the bottom-right cell of the current cell.
#returns the damage taken.
def bottomright(i,j,N,thickness, type_data, below_type):
    if j==N-1: return 0
    if i==thickness-1:
        if rank==worker: return 0
        else:
            if below_type[j+1] == 'o': return 1
            else: return 0

    else:
        if type_data[i+1][j+1] == 'o': return 1
        else: return 0

#printer function prints the final map.
def printer(type_arr, fout, N):
    
    for i in range(N):
        last=''
        for j in range(N):
            last+=type_arr[i][j]+' '
        lasti = last[:len(last)-1]
        fout.write(lasti)
        if(i != N-1): fout.write('\n')


comm = MPI.COMM_WORLD

size = comm.Get_size()
rank = comm.Get_rank()

worker = size-1 #number of worker processes.

if rank==0: #manager process.

    inp = sys.argv[1]
    out = sys.argv[2]

    f = open(inp, 'r')

    #get input.
    lines = f.readlines()
    first = lines[0].split()

    N=int(first[0])
    W=int(first[1])
    T=int(first[2])
    
    #thickness is the number of lines of map for each worker process.
    thickness =  int(N/worker)
    
    health_arr = [[0 for m in range(N)] for j in range(N)] #stores health info.
    type_arr = [['.' for m in range(N)] for j in range(N)] #stores tower type info.

    #send the number of waves to every process.
    for don in range(1,size):
        comm.send(W, dest=don, tag=4)
    
    #iterate for every wave.
    for i in range(W):

        newstro = lines[2*i+1].split(',') #locations of o towers.
        newstrp = lines[2*i+2].split(',') #locations of + towers.

        #place towers onto grid.
        for k in range(T):
            xo = int(newstro[k].split()[0])
            yo = int(newstro[k].split()[1])

            xp = int(newstrp[k].split()[0])
            yp = int(newstrp[k].split()[1])

            #if there exists another tower, do nothing.
            if(health_arr[xo][yo] <= 0):
                health_arr[xo][yo] = 6
                type_arr[xo][yo] = 'o'

            #if there exists another tower, do nothing.
            if(health_arr[xp][yp] <= 0):
                health_arr[xp][yp] = 8
                type_arr[xp][yp] = '+'

        #iterate for every round.
        for rou in range(8):

            #send info to every processor.
            for r in range(1, size):

                comm.send(type_arr[thickness*r-thickness : thickness*r], dest=r, tag=12)
                comm.send(thickness, dest=r, tag=10)
                comm.send(health_arr[thickness*r-thickness : thickness*r], dest=r, tag=11)
                comm.send(N,dest=r,tag=13)
            
            #receive info from every processor.
            for r in range(1,size):

                h_data = comm.recv(source=r, tag=1000) #h_data is the received health info.
                t_data = comm.recv(source=r, tag=1001) #t_data is the received type info.
                
                #rearrange the health map.
                for x in range(thickness):
                    health_arr[(r-1)*thickness + x] = h_data[x]

            #rearrange the type map.
            for h in range(N):
                for g in range(N):
                    if health_arr[h][g] <= 0: type_arr[h][g] = '.'

    #output streamer.
    fout = open(out, 'w')
    #output printer.
    printer(type_arr, fout, N)

else: #worker process.
    
    W = comm.recv(source=0, tag=4) #W is the number of waves.

    #iterate for every wave.
    for dawaves in range(W):

        #iterate for every round.
        for daround in range(8):

            #receive info from manager.
            thickness = comm.recv(source=0, tag=10)
            health_data = comm.recv(source=0, tag=11)
            type_data = comm.recv(source=0, tag=12)
            N = comm.recv(source=0,tag=13)

            #these four list is used for communication with other worker processes.
            above_health=[]
            above_type=[]
            below_health=[]
            below_type=[]

            #for odd numbered workers, first send then get.
            if rank%2 == 1:

                if rank==1: #if rank is 1, just send to and get from below.

                    comm.send(health_data[thickness-1], dest=rank+1, tag=rank*10+1) #send below
                    comm.send(type_data[thickness-1], dest=rank+1, tag=rank*10+6)   #send below
                    
                    below_health = comm.recv(source=rank+1, tag=(rank+1)*10+0)  #get below
                    below_type = comm.recv(source=rank+1, tag=(rank+1)*10+5)    #get below
                    

                elif rank==worker: #if rank is worker, just send to and get from above.

                    comm.send(health_data[0], dest=rank-1, tag=rank*10+0)   #send above
                    comm.send(type_data[0], dest=rank-1, tag=rank*10+5)     #send above
                    
                    above_health = comm.recv(source=rank-1, tag=(rank-1)*10+1)  #get above
                    above_type = comm.recv(source=rank-1, tag=(rank-1)*10+6)    #get above
                    
                else: #send and get.

                    comm.send(health_data[0], dest=rank-1, tag=rank*10+0)   #send above
                    comm.send(type_data[0], dest=rank-1, tag=rank*10+5)     #send above
                    
                    comm.send(health_data[thickness-1], dest=rank+1, tag=rank*10+1) #send below
                    comm.send(type_data[thickness-1], dest=rank+1, tag=rank*10+6)   #send below
                    
                    above_health = comm.recv(source=rank-1, tag=(rank-1)*10+1)  #get above
                    above_type = comm.recv(source=rank-1, tag=(rank-1)*10+6)    #get above
                    
                    below_health = comm.recv(source=rank+1, tag=(rank+1)*10+0)  #get below
                    below_type = comm.recv(source=rank+1, tag=(rank+1)*10+5)    #get below
            
            #for even numbered workers, first get then send.        
            else:
                
                if rank==worker: #if rank is worker, just send to and get from above.
                    
                    above_health = comm.recv(source=rank-1, tag=(rank-1)*10+1)  #get above
                    above_type = comm.recv(source=rank-1, tag=(rank-1)*10+6)    #get above
                    
                    comm.send(health_data[0], dest=rank-1, tag=rank*10+0)   #send above
                    comm.send(type_data[0], dest=rank-1, tag=rank*10+5)     #send above
                    
                else: #send and get.

                    above_health = comm.recv(source=rank-1, tag=(rank-1)*10+1)  #get above
                    above_type = comm.recv(source=rank-1, tag=(rank-1)*10+6)    #get above

                    below_health = comm.recv(source=rank+1, tag=(rank+1)*10+0)  #get below
                    below_type = comm.recv(source=rank+1, tag=(rank+1)*10+5)    #get below
                    
                    comm.send(health_data[0], dest=rank-1, tag=rank*10+0)   #send above
                    comm.send(type_data[0], dest=rank-1, tag=rank*10+5)     #send above

                    comm.send(health_data[thickness-1], dest=rank+1, tag=rank*10+1) #send below
                    comm.send(type_data[thickness-1], dest=rank+1, tag=rank*10+6)   #send below 

            #calculate damage and reduce the health for every cell.
            for i in range(thickness):
                for j in range(N):

                    #if tower is +, check for every side.
                    if(type_data[i][j] == '+'):
                        damage = left(i,j,type_data,'+') + right(i,j,N,type_data,'+') + up(i,j,type_data, '+', above_type) + down(i,j,thickness,type_data, '+', below_type) + topleft(i,j,type_data,above_type) + topright(i,j,N,type_data,above_type) + bottomleft(i,j,thickness,type_data,below_type) + bottomright(i,j,N,thickness, type_data, below_type)

                        health_data[i][j] = health_data[i][j] - damage

                    #if tower is o, just check left, right, up, and down.
                    elif(type_data[i][j] == 'o'):
                        damage = left(i,j,type_data,'o') + right(i,j,N,type_data,'o') + up(i,j,type_data, 'o', above_type) + down(i,j,thickness,type_data, 'o', below_type)

                        health_data[i][j] = health_data[i][j] - damage

            #send the info back to the manager.
            comm.send(health_data, dest=0, tag=1000)
            comm.send(type_data, dest=0, tag=1001)
            

