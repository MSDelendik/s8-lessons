USER_VM=yc-user
HOST_VM=158.160.32.189
IMAGE_VM=$(ssh -i ssh_private_key.file $USER_VM@$HOST_VM docker ps --format '{{.Names}}')
echo USER_VM $USER_VM
echo HOST_VM $HOST_VM
echo IMAGE_VM $IMAGE_VM
scp -ri ssh_private_key.file /Users/user/Desktop/ya_practicum/git/s8-lessons/your-folder $USER_VM@$HOST_VM:~/
ssh -i ssh_private_key.file $USER_VM@$HOST_VM "
    docker cp ~/ $IMAGE_VM:/lessons/
" 