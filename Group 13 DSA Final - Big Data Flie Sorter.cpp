/*Group 13 DSA Final - Big Data Flie Sorter
- using External Merge Sort  , Heap Sort for intial chunking
*/ 
#include <bits/stdc++.h>
#include <sys/stat.h> // lấy thông tin file (kích thước)
#ifdef _WIN32
#include <direct.h> // tạo thư mục trên windows(_mkdir)
#endif
using namespace std;
using namespace chrono; // chạy benchmark
//chunk size : 100MB
//note : Giả lập giới hạn Ram , External Sort solves file input > ram 
const size_t CHUNK_SIZE = 100*1024 * 1024;
//Priority Queue in merge stage
struct MergeElement{
    int value ; // gía trị hiện tại ; 
    int chunkIndex; // thuộc về file chunk - > đọc tiếp từ file đó 
    //override toán tử - > min -heap (phần tử nhỏ nhất lên đầu )
    //priority_queue mặc định là max-heap nên cần đảo ngược so sánh 
    bool operator>(const MergeElement &other) const {
        return value > other.value  ; 

    }
};
class ExternalMergeSort{
    private:
    string outputFile, tempDir ,inputFile ; 
    vector <string> chunkFiles;
    //vector dùng đẻ lưu danh sách tên các file tạm (chunk );
    //hàm phụ trợ : lấy kích thước file 
    size_t getFileSize(const string& filename){
        struct stat st; 
        return (stat(filename.c_str(),&st) ==  0 ) ?st.st_size : 0 ;
    }
    //HeapSort O(N log N ) -> không tốn nhiều bộ nhớ phụ như MergeSort 
      void heapSort(vector<int> & arr){
        make_heap(arr.begin(), arr.end()) ; 
        sort_heap(arr.begin(),arr.end()) ;// sắp xếp vector dựa trên heap 

    }
    //ghi 1 chunk (sorted) từ RAM xuống ổ cứng 
    void writeSortedChunk(vector <int> & chunk , const string& filename ){
        heapSort(chunk);  // sắp xếp trước khi ghi 
        ofstream out(filename, ios::binary ) ; // mở file chế độ binary để tốc độ nhanh nhất
        // ghi lại toàn bộ vector xuống file một lần để tối ưu I/O
        out.write((char*)chunk.data(),chunk.size()*sizeof(int)) ;
        chunkFiles.push_back(filename) ; 
        //lưu tên file vào danh sách để sau merge


    }
    //Sorted RUns - tạo chunk 
    // đọc file lớn -  > chia thành phần vừa vưới RAM (100MB) , sắp xếp và ghi tạm 
    void createSortedChunks(){
        ifstream in (inputFile,ios::binary);
        vector <int> chunk ; 
        //Reserve bộ nhớ trước để tránh việc vector phải cấp phát lại
        chunk.reserve(CHUNK_SIZE/sizeof(int)) ;
        int value , chunkCount =0 ; 
        cout << "Creating sorted Chunks" << endl; 
        //Đọc từng số nguyên từ file input
        while(in.read((char*)& value ,sizeof(int))){
            chunk.push_back(value) ; 
            //nếu buffer trong ram đã đầy đạt 100mb
            if(chunk.size() *sizeof(int) >= CHUNK_SIZE){
                //ghi ra file tạm :
                writeSortedChunk(chunk,tempDir +"/chunk_"+to_string(chunkCount++) +".dat") ;
                chunk.clear() ; // xóa buffer nhận dữ liệu mới 


            }

        }
        // XỬ lý dữ liệu còn dư cuối cùng 
        if(!chunk.empty()){
            writeSortedChunk(chunk , tempDir +"/chunk_"+ to_string(chunkCount ++)+".dat") ;


        }
        cout <<"Created " << chunkCount <<"chunks" <<endl; 
        
    }
    //K-Way Merge : trộn các file tạm ;
    //sử dụng min-heap để tìm phần tư nhỏ nhất trong k file cùng lúc 
    void mergeChunks(){
        cout << "Merging chunks" << endl; 
        vector <ifstream*> streams ; 
        // danh sách các luông đọc file (file streams) ;
        // mở tất cả các file chunk có cùng lúc ; 
        // nếu số lượng chunk quá lớn , cần thuật toán merge nhiều vòng , multi-pass merge 
        for(int i = 0 ; i < chunkFiles.size();i++){
            streams.push_back(new ifstream(chunkFiles[i],ios::binary)) ; 

        }
        //min-heap lưu trữ phần tử hiện tại nhỏ nhất của mỗi chunk 
        priority_queue<MergeElement, vector<MergeElement>,greater<MergeElement>> pq ;
        //Khởi tạo Heap : đọc số đầu teien của mỗi file chunk và đẩy vào heap 
        for(int i = 0  ; i < streams.size(); i++){
            int value ; 
            if(streams[i] ->read((char*)&value, sizeof(int))){
                pq.push({value,i}) ; //lưu giá trị và chị số file nguồn 
            }
        }
        ofstream out(outputFile,ios::binary) ;//file kết quả cuối cùng 
        auto start = high_resolution_clock:: now() ; 
        size_t count = 0 ; 
        // vòng lặp chính của thuật toán Merge 
        while(!pq.empty()){
            // 1 lấy phần tử nhỏ nhất hiện có trong heap 
            auto minElem = pq.top() ; 
            pq.pop() ; 
            //2 ghi phần tử đó vào file kết quả ; 
            out.write((char*)& minElem.value , sizeof(int)) ; 
            count ++ ; 
            //3 Đọc phần tử tiếp theo từ đúng file chunk mà ta vừa lất sổ ra 
            int nextValue ;
            if(streams[minElem.chunkIndex] ->read((char*)&nextValue,sizeof(int))){
                //nếu fiel đó còn dữ liệu đẩy số mới vào heap để tiếp tục compare
                pq.push({nextValue,minElem.chunkIndex});
                
            }
            //nếu file đó hết dữ liệu - > không push gì cả 

        }
        out.close() ; 
        auto end = high_resolution_clock::now() ;
        auto duration = duration_cast<milliseconds>(end - start) ; 
        //đóng và giải phóng bộ nhớ các luồng file;
        for(auto stream : streams){
        stream -> close() ; 
        delete stream ; 


    }
    cout << "Merge complete: " << count << "elements" << endl ; 
    cout << "Merge time : " << duration.count() <<"ms" << endl; 
}
    //cleanup
    void cleanup(){
        for(auto& file :chunkFiles) 
        remove(file.c_str()) ;

    }
    public:
    //constructor 
    ExternalMergeSort(string in , string out , string temp = ".temp"):inputFile(in) ,outputFile(out),tempDir(temp) {
        #ifdef _WIN32
        _mkdir(tempDir.c_str()) ; //tạo thư mục trên windows
        #else
        mkdir(tempDir.c_str(),0755);//max  , linux 
        #endif
    }
    //hàm public chính để chạy toàn bộ quy trình 
    void sort()
    {
      cout <<"File size:" << getFileSize(inputFile) / 1024.0 * 1024.0 << "MB" << endl; 
        createSortedChunks() ; 
        //step1 : chia và sắp xếp cục bộ 
        mergeChunks() ;
        //step2 ; merge ;
        cleanup() ; 
        // dọn rác 
        cout << "Sorting Completed " << endl; 
    }


};
// hàm tạo dữ liệu giả 
void createTestFile(string filename , size_t numInterges){
    ofstream file(filename , ios::binary);
    cout <<"Creating test file with" << numInterges << "random integers" << endl ;
    srand(time(0)) ; 
    for(size_t i = 0 ; i < numInterges  ; i++) {
        int value = rand() % 1000000 ;
        file.write((char*) & value ,sizeof(int)) ;

    }
    cout << "Flie created " << endl; 

}
// hàm kiểm tra tính đúng đắn : duyệt file output xem số sau có lớn hơn số trước không 
bool verifySorted(string filename){
    ifstream file(filename, ios::binary) ; 
    int prev , curr ;
    bool first = true ; 
    while(file.read((char*) & curr, sizeof(int))){
        if(!first && curr < prev ) {
            //lỗi : số sau nhỏ hơn số trước 
            cout <<"Not Sorted " << endl;
            return false ;

        }
        prev = curr ;
        first = false ; 

    }
    cout << "File is correctly sorted " << endl ;
    return true ; 
}
    void printSample(string filename, int numElements = 50 ) {
        ifstream file(filename,ios::binary); 
        file.seekg(0,ios::end);// di chuyển con trỏ xuống dưới file để lấy kích thước .
        size_t fileSize = file.tellg() ; 
        size_t totalElements = fileSize/sizeof(int) ; 
        file.seekg(0,ios::beg); // back to beginning 
        cout << "Sample from " << filename << endl ;
        cout << "Total elements " << totalElements << endl;
        cout << "First" << numElements << "elements" << endl; 
        int value ; 
        for(int i = 0 ; i < numElements && file.read((char*) & value ,sizeof(int));i++){
            cout <<value << "" ; 
            if((i+1) %10 == 0 ) cout << endl; 

        }
        cout <<"Last" << numElements <<"elements:" << endl; 
        size_t lastStart = max((size_t)0 ,totalElements - numElements) ;
        file.seekg(lastStart * sizeof(int),ios::beg) ;
        for(int i = 0  ; i < numElements && file.read ((char *)&value , sizeof(int)) ; i++){
            cout << value << " " ; 
            if((i+1)%10 == 0 ) cout << endl ; 

        }
        cout << endl ;

    }
    //chuyển file binary sang text .txt
    void exportToText(string binaryFile , string textFile){
        ifstream in(binaryFile, ios::binary);
        ofstream out(textFile); 
        cout << "Exporting to " << textFile << endl ; 
        auto start = high_resolution_clock::now() ; 
        int value , count = 0  ;
        while(in.read((char*) &value , sizeof(int))) {
            out << value << "\n" ;
            count ++ ;

        }
        out.close() ;
        auto end = high_resolution_clock::now() ; 
        auto duration = duration_cast <milliseconds>(end-start) ; 
       cout << "Exported " << count << "interger to " << textFile<< endl; 
        cout << "Export Time : " << duration.count() << "ms" <<endl; 
        
    }
    int main(int argc, char* argv[]){
        //nếu chạy kông có tham số dòng lênh - > test tự đọngo
        if(argc < 3 ) {
            createTestFile("unsorted.dat",10000000); //~40mb
            ExternalMergeSort sorter("unsorted.dat" ,"sorted.dat") ; 
            sorter.sort() ; 
            verifySorted("sorted.dat") ;
            printSample("unsorted.dat",20) ; 
            exportToText("sorted.dat","sorted.txt");
            return 0 ; 

        }
        ExternalMergeSort sorter(argv[1],argv[2]) ;
        verifySorted(argv[2]); 
        printSample(argv[1] ,20) ; 
        printSample(argv[2],20);
        exportToText(argv[2],"sorted.txt") ;
        return 0 ; 
    }

