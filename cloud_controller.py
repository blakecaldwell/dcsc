#!/usr/bin/env /usr/bin/python

# http://www.takaitra.com/posts/384

from optparse import OptionParser
from boto.ec2.connection import EC2Connection
from boto.ec2.regioninfo import RegionInfo
import sys, os, re, time, gzip

def parse_url(url):
    "Extract the components of a URL path for EC2/S3"
    match = re.match("http://(.*?):(\d+)(/(.*$))?", url)
    result = None
    if match and match.groups >= 2:
        host = match.group(1)
        port = int(match.group(2))
        path = match.group(3)
        if path == None:
            path = ""
        result = (host, port, path)
    return result

def create_openstack_connection ():
    EC2_ACCESS  = os.getenv('EC2_ACCESS_KEY')
    EC2_SECRET  = os.getenv('EC2_SECRET_KEY')
    EC2_URL =parse_url(os.getenv('EC2_URL'))
    ZONE='india'
    
    openstack_region = RegionInfo(name="openstack", endpoint=EC2_URL[0])
    conn = EC2Connection(EC2_ACCESS, EC2_SECRET, region=openstack_region, port=int(EC2_URL[1]),path=EC2_URL[2],is_secure=False)
    return conn

def create_openstack_reservation (conn, IMAGE, INSTANCE_TYPE, KEY_NAME, user_data_file, num_instances):

    if not user_data_file:
	reservation = conn.run_instances(IMAGE, instance_type=INSTANCE_TYPE, key_name=KEY_NAME,security_groups=['mmonaco-sg'],min_count=num_instances,max_count=num_instances)
    else:
        user_data_fd = open(user_data_file, 'r')
	reservation = conn.run_instances(IMAGE, instance_type=INSTANCE_TYPE, key_name=KEY_NAME, user_data=user_data_fd.read(),security_groups=['mmonaco-sg'],min_count=num_instances,max_count=num_instances)
        user_data_fd.close()
    return reservation
    
def find_my_instances(connection, key, verbose=False):
    mine = []
    reservations = connection.get_all_instances()
    for resv in reservations:
        for instance in resv.instances:
            if instance.key_name == key:
                if verbose:
                    print "Found key ", instance.key_name, "for instance", instance
                mine.append(instance)
    return mine

def wait_for_instance_to_be_public(instance, verbose=False):
    while True:
        if instance.state == 'running' and (instance.ip_address != instance.private_ip_address):
            return True
        else:
            if verbose:
                print "Waiting", (instance.state, instance.ip_address, instance.private_ip_address)
            time.sleep(3)
            instance.update()

def wait_for_reservation_to_be_public(reservation, verbose=False):
    for instance in reservation.instances:
        wait_for_instance_to_be_public(instance, verbose)

def kill_my_instances(connection, key, verbose=True):
    mine = find_my_instances(connection, key, verbose)
    ids = [instance.id for instance in mine]
    if len(ids) > 0:
        if verbose:
            print "Terminating instances", ids
        connection.terminate_instances(ids)

def insert_queue_server(infile,server_ip):
 
    modified_file = infile+".gz"

    infd= open(infile)
    outfd = gzip.open(modified_file, "wt")
    for line in infd:
        outfd.write( line.replace('inserted_by_cloud_controller', server_ip) )
    infd.close()
    outfd.close()

    return modified_file

def main(*args):
    use = "Usage: %prog [number of workers]"
    parser = OptionParser(usage = use)
    parser.add_option("--terminate", action="store_true", dest="terminate", default=False, help="terminate all of my instances")
    (options, args) = parser.parse_args()

    conn = create_openstack_connection()
    KEY_NAME        = 'dcsc_bc'

    if (options.terminate):
        kill_my_instances(conn,KEY_NAME)
        sys.exit(0)

    if len(args) != 1:
        parser.error("incorrect number of arguments")

    try:
        N=sys.argv[1]
        print "starting queue server and " + N + " workers"

        IMAGE       = 'ami-0000000d'
        client_user_data_file  = 'combined-userdata-client.txt.gz'
        INSTANCE_TYPE   = 'm1.tiny'


        # start the queue server
	server_user_data_file 	= 'combined-userdata-server.txt.gz'
	num_instances = 1
        server_reservation = create_openstack_reservation(conn,IMAGE, INSTANCE_TYPE, KEY_NAME,server_user_data_file,num_instances)
        server_instance = server_reservation.instances[0]
        print "Wait for queue server"
        wait_for_reservation_to_be_public(server_reservation, True)
        server_ip = server_instance.ip_address
        print 'Started the queue server: {0}'.format(server_ip)
        server_private_ip = server_instance.private_ip_address

	# modify QHOST
        client_user_data_file  = 'combined-userdata-client.txt'
        user_data_to_client = insert_queue_server(client_user_data_file,server_private_ip)   
 
        # start the workers
	num_instances = int(N)
	worker_reservation = create_openstack_reservation(conn,IMAGE, INSTANCE_TYPE, KEY_NAME,user_data_to_client,num_instances)
        print "Wait for workers"
        wait_for_reservation_to_be_public(worker_reservation, True)

        for i in range(num_instances):
          worker_instance = worker_reservation.instances[i]
          print 'Started the instance: {0}'.format(worker_instance.dns_name)

    except Exception, err:
         sys.stderr.write('ERROR: %s\n' % str(err))
         print "terminating instances"
         kill_my_instances(conn,KEY_NAME)
         sys.exit(1)

    return 0

if __name__ == '__main__':
         sys.exit(main(*sys.argv))
