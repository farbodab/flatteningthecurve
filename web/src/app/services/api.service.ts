import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';

@Injectable({
  providedIn: 'root'
})
export class ApiService {

  readonly api_endpoint : string = 'https://ihs-api.herokuapp.com/covid/allc'

  constructor(private http_client : HttpClient) { 

  }

  get_graph_data() {

  }
}
