package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"time"
)

//GOLANG 实现大数据多管道多机器并行处理 排序问题 使用归并排序
func main(){
	var filename  = "small.in"
	var count = 10

	var p = CreatePipeline(filename,count*8,2)

	//输出前10
	var count_ = 0
	for v:= range p{
		count_++
		fmt.Println(v)

		if count_>=10 {
			break
		}
	}
}

var startTime  time.Time //统计时间

func Init(){
	startTime = time.Now()
}
//创建处理数据的管道
//fileSize 文件大小（字节） chunkCount 线程
func CreatePipeline(filename string,fileSize,chunkCount int) <-chan int {

	Init()

	var chunkSize = fileSize/chunkCount

	var sortResult []<-chan int

	var addrResult []string

	for i:=0;i<chunkCount;i++{
		var file,err = os.Open(filename)
		if err!=nil{
			panic(err)
		}

		file.Seek(int64(i*chunkSize),0)

		var source = ReadFile(file,chunkSize)//读文件

		var sorted = InMemSort(source)//排序

		var addr = ":"+strconv.Itoa(7000+i)//模拟生成不同的机器

		NetworkWrite(addr,sorted)//排好序的数据写入机器

		addrResult  = append(addrResult,addr)

	}
	//从机器拉取数据
	for _,address := range addrResult{
		sortResult  = append(sortResult,NetworkRead(address))
	}
	//归并排序数据
	return MergeN(sortResult ...)
}


//写入机器
func NetworkWrite(addr string,in <-chan int){
	var listner,err = net.Listen("tcp",addr)

	if err!=nil{
		panic(err)
	}

	go func() {
		defer listner.Close()

		var conn,error = listner.Accept()

		if error!=nil{
			panic(error)
		}

		defer conn.Close()

		var writer = bufio.NewWriter(conn)

		defer writer.Flush()
		
		WriteFile(writer,in)
	}()

}


//从机器拉取数据
func NetworkRead(addr string) <-chan int  {
	var out = make(chan int)
	go func() {
		var conn,err = net.Dial("tcp",addr)

		if err!=nil{
			panic(err)
		}

		defer conn.Close()

		var r = ReadFile(bufio.NewReader(conn),-1)

		for v:=range r{
			out<-v
		}
		close(out)
	}()
	return out
}


//排序
func InMemSort(a <-chan int) <-chan int {
	var out = make(chan int)

	go func() {
		var intArr []int
		for v := range a{
			intArr = append(intArr,v)
		}
		fmt.Println("append Done:",time.Now().Sub(startTime))
		sort.Ints(intArr)
		fmt.Println("sort Done:",time.Now().Sub(startTime))
		for _,v := range intArr{
			out<-v
		}
		close(out)
	}()

	return out
}

//归并
func Merge(in1,in2 <-chan int) <-chan int {

	var out = make(chan int)

	go func() {
		var v1,ok1 = <-in1
		var v2,ok2 = <-in2
		for ok1 || ok2 {
			if !ok2 || (ok1 && v1<=v2) {
				out <- v1
				v1,ok1 = <-in1
			}else{
				out <- v2
				v2,ok2 = <-in2
			}
		}
		close(out)
		fmt.Println("Merge Done:",time.Now().Sub(startTime))
	}()

	return out
}

//递归归并
func MergeN(inputs... <-chan int) <-chan int{
	var m = len(inputs)/2
	if len(inputs) == 1 {
		return inputs[0]
	}
	return Merge(MergeN(inputs[:m]...),MergeN(inputs[m:]...))
}

//写
func WriteFile(filename io.Writer,in <-chan int)  {
	for v:= range in {
		buffer:=make([]byte,8)
		binary.BigEndian.PutUint64(buffer,uint64(v))
		filename.Write(buffer)
	}
}
//读
func ReadFile(filename io.Reader,chunkSize int) <-chan int{
	var reader  = make(chan int)
	go func() {
		buffer := make([]byte,8)
		var readSize = 0
		for{
			n,err:=filename.Read(buffer)
			if n>0 {
				var number  =int(binary.BigEndian.Uint64(buffer))
				reader <- number
			}
			readSize +=n
			if err!=nil || readSize >= chunkSize ||  chunkSize!=-1{
				break
			}
		}
		close(reader)
	}()
	return reader
}
