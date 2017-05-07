using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using UnityEngine;

namespace AR.Core.Types
{

    [Serializable]
    public class Neo4jGraph_SimpleJson
    {
        public String[] columns;
        public data[][] data;
    }

    public class Neo4jGraph
    {
        public String[] columns;
        public data[,] data;
    }


    [Serializable]
    public class data
    {

        public String[] nodes;
        public String[] directions;
        public String[] relationships;
        public int length;
        public String start;
        public String end;

    }

    //Serializable for final project

    [Serializable]
    public class Neo4jGraph_CompleteJson
    {
        public String[] columns;
        public dataList[][] data;
    }


    [Serializable]
    public class dataList
    {
        public MetaData metaData;
        public DataComplete dataComplete;
        public String paged_traverse;
        public String outgoing_relationships;
        public String outgoing_typed_relationships;
        public String labels;
        public String create_relationship;
        public String traverse;
        public object extensions;
        public String all_relationships;
        public String all_typed_relationships;
        public String property;
        public String self;
        public String incoming_relationships;
        public String properties;
        public String incoming_typed_relationships;

    }

    [Serializable]
    public class DataComplete
    {
        public String taxonomyCode;
        public String lastName;
        public String zipCode;
        public String gender;
        public String licenseState;
        public String city;
        public String typeCode;
        public String firstName;
        public String phoneNumber;
        public String stateName;
        public String addressLine1;
        public String middleName;
        public String licenseNumber;
        public String crediantial;


    }




    [Serializable]
    public class MetaData
    {
        public String[] id;
        public String[] labels;
    }














}
