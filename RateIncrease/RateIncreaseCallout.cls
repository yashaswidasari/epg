public class RateIncreaseCallout implements Queueable, Database.AllowsCallouts {

    public class IncreaseRequestModel {
        public String quoteId;
        public Integer service;
        public Decimal increase;
        public Decimal pickup;
        public Decimal margin;
        public Boolean passthrough;
    }

    private IncreaseRequestModel[] increases;
    private String custno;
    private String custName;
    private String quoteNum;
    private String quoteDate;
    private Boolean saveRates;

    public RateIncreaseCallout(IncreaseRequestModel[] increases,
            String custno,
            String custName,
            String quoteNum,
            String quoteDate) {
        this.increases = increases;
        this.custno = custno;
        this.custName = custName;
        this.quoteNum = quoteNum;
        this.quoteDate = quoteDate;
        this.saveRates = true;
    }

    class IncreasePcLbResponseModel {
        public Integer service;
        public String quoteId;
        public Pc_Lb_Rate__c[]  rates;
    }

    class CardServicesResponseModel {
        public Integer service;
        public String quoteId;
    }

    class IncreaseRateCardResponseModel {
        public String filename;
        public String content;
        public String type;
        public Boolean success;
        public CardServicesResponseModel[] services; 
        public Id[] relatedQuotes;
    }

    class ServerResponse {
        public IncreasePcLbResponseModel[] pcLbRates;
        public IncreaseRateCardResponseModel[] rateCards;
    }

    public void execute(QueueableContext context) {
        HttpRequest request = new HttpRequest();

        String endpoint = 'https://uw1-prd-func-rate-ratemanagement.azurewebsites.net/api'
            + '/GetIncreaseRates'
            + '?code=p9Iqr9bLLow9AOM4lqExFePAVnhHXNW5S1iluDFMAexpAzFueA65Hg==';

        request.setEndpoint(endpoint);
        request.setMethod('POST');
        request.setTimeout(120000);
        request.setBody(JSON.serialize(this));
        HttpResponse response = new HTTP().send(request);

        ServerResponse rates = new ServerResponse();
        rates = (ServerResponse) JSON.deserialize(
        	response.getBody(), ServerResponse.class
        );

        System.debug(rates);

        if (rates.pcLbRates != null) {
            insertPcLbRates(rates.pcLbRates);
        }
        if (rates.rateCards != null) {
            insertRateCards(rates.rateCards);
        }

    }

    public Quoted_Service__c[] updateQuoteStatuses(Set<Id> quoteIds, String quoteType, String status){
        Quoted_Service__c[] updatedQuoteStatuses = [
            SELECT Id, Status__c
            FROM Quoted_Service__c
            WHERE Id IN :quoteIds
            FOR UPDATE
        ];

        for (Quoted_Service__c quote : updatedQuoteStatuses){
            quote.Status__c = status;
            quote.QuoteType__c = quoteType;
        }

        return updatedQuoteStatuses;
    }

    public Quoted_Service__c[] updateQuoteStatusesDocs(Map<Id, String> quoteStatusUpdates, Map<Id,String> quoteDocUpdates, String quoteType) {
        Quoted_Service__c[] updatedQuoteStatuses = [
            SELECT Id, Status__c
            FROM Quoted_Service__c
            WHERE Id IN :quoteStatusUpdates.keySet()
            FOR UPDATE
        ];

        for (Quoted_Service__c quote : updatedQuoteStatuses){
            quote.Status__c = quoteStatusUpdates.get(quote.Id);
            quote.QuoteType__c = quoteType;
            String docSearch = quoteDocUpdates.get(quote.Id);
            if (docSearch != null){
                quote.CustDocId__c = docSearch;
            }
        }

        return updatedQuoteStatuses;
    }

    public void insertPcLbRates(IncreasePcLbResponseModel[] pcLbRates) {
        Pc_Lb_Rate__c[] allRates = new Pc_Lb_Rate__c[] {};
        Set<Id> quoteIds = new Set<Id>();

        for (IncreasePcLbResponseModel service : pcLbRates) {
            if (service.quoteId != null) {
                quoteIds.add(service.quoteId);
                for (Pc_Lb_Rate__c rate : service.rates) {
                    rate.Quoted_Service__c = service.quoteId;
                    allRates.add(rate);
                }
            }
        }

        Quoted_Service__c[] updatedQuoteStatuses = updateQuoteStatuses(quoteIds, 'lineitem', 'Pending Sales Review');

        try {
            insert allRates;
            update updatedQuoteStatuses;
        } catch(DmlException e) {
            System.debug('An unexpected error has occurred: ' + e.getMessage());
        }
    }

    public void insertRateCards(IncreaseRateCardResponseModel[] rateCards) {
        ContentVersion[] rateCardFiles = new ContentVersion[] {};
        Id[] rateCardFileIds = new Id[] {};
        ContentDocumentLink[] rateCardLinks = new ContentDocumentLink[] {};

        for (IncreaseRateCardResponseModel rates: rateCards) {
            if (rates.success) {
                ContentVersion version = new ContentVersion();
                version.ContentLocation = 'S';
                version.PathOnClient = rates.filename;
                version.Title = rates.filename;
                version.versionData = EncodingUtil.base64Decode(rates.content);
                version.Upload_Type_fileupload__c = 'SafeForCustomer';
                rateCardFiles.add(version);
            }
        }

        insert rateCardFiles;

        for (ContentVersion file: rateCardFiles) {
            rateCardFileIds.add(file.Id);
        }
        ContentVersion[] versionDocs = [SELECT Title, ContentDocumentId FROM ContentVersion WHERE id IN :rateCardFileIds];
        
        Map<String,Id> docIds = new Map<String,Id>();
        for (ContentVersion version: versionDocs) {
            docIds.put(version.Title, version.ContentDocumentId);
        }

        Map<Id, String> quoteStatusUpdates = new Map<Id, String>();
        Map<Id, String> quoteCustDocs = new Map<Id, String>();
        
        for (IncreaseRateCardResponseModel rates: rateCards) {
            if (rates.success){
                Id versionDoc = docIds.get(rates.filename);
                for (Id quoteId: rates.relatedQuotes){
                    ContentDocumentLink versionLink = new ContentDocumentLink();
                    versionLink.ContentDocumentId = versionDoc;
                    versionLink.LinkedEntityId = quoteId;
                    versionLink.shareType = 'I';
                    rateCardLinks.add(versionLink);
                    quoteStatusUpdates.put(quoteId, 'Pending Sales Review');
                    quoteCustDocs.put(quoteId, versionDoc);
                }
            } else {
                for (Id quoteId: rates.relatedQuotes){
                    quoteStatusUpdates.put(quoteId, 'Auto Retrieval Failed');
                }
            }
            
        }

        Quoted_Service__c[] updatedQuoteStatuses = updateQuoteStatusesDocs(quoteStatusUpdates, quoteCustDocs, 'standard');

        insert rateCardLinks;
        update updatedQuoteStatuses;
            
    }

}

