$(document).ready(function(){


    $.getJSON('/fetch_reports', async function(){

        const posts_array = await $.ajax('/fetch_reports')


        // const name = response.map(a=>a[0])
        // const probability = response.map(a=>a[1])
        // const prediction = response.map(a=>a[2])
        
        //var posts_array = JSON.parse(response);
        // console.log(posts_array)
        
        var columns = ['Name', 'Probability', 'Predicition']
        var table_html = '';
        for (var i = 0; i < posts_array.length; i++)
        {
            //create html table row
            if(posts_array[i][1] >= 0.50){
                table_html += '<tr class="table-danger">';
                for( var j = 0; j < columns.length; j++ ){
                    //create html table cell, add class to cells to identify columns          
                    table_html += '<td class="' +columns[j]+ '" >'  + posts_array[i][j] + '</td>'
                }
                table_html += '</tr>'
            }
            else if(posts_array[i][1] >= 0.45 && posts_array[i][1] < 50){
                table_html += '<tr class="table-warning">';
                for( var j = 0; j < columns.length; j++ ){
                    //create html table cell, add class to cells to identify columns          
                    table_html += '<td class="' +columns[j]+ '" >'  + posts_array[i][j] + '</td>'
                }
                table_html += '</tr>'
            }
            else{
                table_html += '<tr class="table-success">';
                for( var j = 0; j < columns.length; j++ ){
                    //create html table cell, add class to cells to identify columns          
                    table_html += '<td class="' +columns[j]+ '" >'  + posts_array[i][j] + '</td>'
                }
                table_html += '</tr>'
            }
        }
        $( "#events" ).append(  table_html );        

    })      
})
        