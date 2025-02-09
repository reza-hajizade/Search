using Elastic.Clients.Elasticsearch;
using Microsoft.AspNetCore.Http.HttpResults;
using Search;
using Search.Infrastructure.Extensions;
using Search.Models;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();


builder.BrokerConfigure();
builder.ElasticSearchConfigure();

builder.Services.Configure<AppSettings>(builder.Configuration);


var app = builder.Build();


app.UseSwagger();
app.UseSwaggerUI();


//app.UseHttpsRedirection();


app.MapGet("/", SearchItems);



static async Task<Results<Ok<IReadOnlyCollection<CatalogItemIndex>>, NotFound>> SearchItems(string text, ElasticsearchClient elasticsearchClient)
 {
    var response = await elasticsearchClient.SearchAsync<CatalogItemIndex>(s => s
          .Index(CatalogItemIndex.IndexName)
          .From(0)
          .Size(10)
          .Query(q => 
          q.Fuzzy(t => t.Field(x => x.Description).Value(text))) 
          
     );


    if (response.IsValidResponse)
        return  TypedResults.Ok(response.Documents);

    return TypedResults.NotFound();
}

app.Run();



